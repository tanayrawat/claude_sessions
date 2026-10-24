from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Callable, Literal

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import inbox, pagination
from .security import TokenBucket, conditional_json, make_guard
from .settings import Settings


class IssueOut(BaseModel):
    id: int
    number: int
    title: str
    state: str
    author_login: str | None
    labels: list[str]
    comments: int
    is_pull_request: bool
    created_at: str
    updated_at: str
    closed_at: str | None


class IssuePage(BaseModel):
    data: list[IssueOut]
    next_cursor: str | None  # null = last page
    limit: int


COLS = "id, number, title, state, author_login, labels, comments, is_pull_request, created_at, updated_at, closed_at"


def _iso(dt) -> str | None:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


def _row(r) -> dict:
    d = {"id": r[0], "number": r[1], "title": r[2], "state": r[3], "author_login": r[4], "labels": list(r[5] or []),
         "comments": r[6], "is_pull_request": r[7], "created_at": _iso(r[8]), "updated_at": _iso(r[9]),
         "closed_at": _iso(r[10])}
    return d


def verify_signature(secret: str, body: bytes, signature_header: str | None) -> bool:
    """LEARN: GitHub signs the RAW request bytes with HMAC-SHA256 using the shared secret.
    Two classic bugs: (1) verifying a re-serialized json.dumps(parsed_body) — whitespace/key order differ, so a
    valid signature fails; (2) comparing with `==` — leaks timing. Use compare_digest on the raw bytes."""
    if not signature_header:
        return False
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected.encode(), signature_header.encode())


def create_app(settings: Settings, clock: Callable[[], float] = time.monotonic) -> FastAPI:
    app = FastAPI(title="Issues data API", version="1.0",
                  description="Serves gold/silver warehouse tables to consumers and receives GitHub webhooks.")
    limiter = TokenBucket(settings.rate_capacity, settings.rate_refill_per_sec, clock)
    guard = make_guard(settings, limiter)

    def warehouse():
        # Open per request and close immediately. DuckDB allows one writer process OR many readers, so holding a
        # connection open would block the pipeline from loading. (Real warehouses don't have this limit — it's a
        # reason serving layers usually sit on Postgres/ClickHouse/Snowflake, or a published snapshot.)
        return duckdb.connect(str(settings.db_path), read_only=True)

    # -- operational endpoints (no auth: load balancers and k8s probes can't send API keys) ---------------------
    @app.get("/health", tags=["ops"])
    def health():
        """Liveness: the process is up. Must NOT touch dependencies, or a DB blip restarts every pod."""
        return {"status": "ok"}

    @app.get("/ready", tags=["ops"])
    def ready():
        """Readiness: can we actually serve? Failing this takes the pod out of rotation without restarting it."""
        try:
            con = warehouse()
            con.execute("SELECT 1").fetchall()
            con.close()
        except Exception:
            raise HTTPException(503, "warehouse unavailable")
        return {"status": "ready"}

    # -- STAGE 1/2: read API -------------------------------------------------------------------------------------
    @app.get("/v1/issues", response_model=IssuePage, tags=["issues"])
    def list_issues(
        request: Request,
        _client: str = Depends(guard),
        state: Literal["open", "closed"] | None = None,
        label: str | None = None,
        include_prs: bool = False,
        limit: int = Query(50, ge=1, le=200),  # ALWAYS cap page size; an unbounded query is a self-inflicted outage
        cursor: str | None = None,
    ):
        where, params = [], []
        if not include_prs:
            where.append("NOT is_pull_request")
        if state:
            where.append("state = ?"); params.append(state)
        if label:
            where.append("list_contains(labels, ?)"); params.append(label)
        if cursor:
            try:
                u, i = pagination.decode(cursor)
            except pagination.BadCursor:
                raise HTTPException(400, "invalid cursor")
            where.append("(updated_at, id) < (CAST(? AS TIMESTAMP), ?)"); params += [u, i]
        # LEARN: values go in as bound parameters (?), never string-formatted into SQL. Column names/ORDER BY are
        # fixed text here. This is the whole defence against SQL injection.
        sql = f"SELECT {COLS} FROM silver.issues {'WHERE ' + ' AND '.join(where) if where else ''} " \
              f"ORDER BY updated_at DESC, id DESC LIMIT ?"
        con = warehouse()
        try:
            rows = con.execute(sql, params + [limit + 1]).fetchall()  # fetch one extra: tells us if there's a next page
        finally:
            con.close()
        page, has_more = rows[:limit], len(rows) > limit
        nxt = pagination.encode(page[-1][9], page[-1][0]) if has_more else None
        return conditional_json(request, {"data": [_row(r) for r in page], "next_cursor": nxt, "limit": limit})

    @app.get("/v1/issues/{issue_id}", response_model=IssueOut, tags=["issues"])
    def get_issue(issue_id: int, request: Request, _client: str = Depends(guard)):
        con = warehouse()
        try:
            rows = con.execute(f"SELECT {COLS} FROM silver.issues WHERE id = ?", [issue_id]).fetchall()
        finally:
            con.close()
        if not rows:
            raise HTTPException(404, "issue not found")
        return conditional_json(request, _row(rows[0]))

    # -- STAGE 3: webhooks ---------------------------------------------------------------------------------------
    @app.post("/webhooks/github", tags=["webhooks"])
    async def github_webhook(
        request: Request,
        x_hub_signature_256: str | None = Header(default=None),
        x_github_delivery: str | None = Header(default=None),
        x_github_event: str | None = Header(default=None),
    ):
        if not settings.webhook_secret:
            raise HTTPException(503, "webhook secret not configured")  # fail closed, never accept unsigned traffic
        body = await request.body()  # RAW bytes — before any JSON parsing
        if not verify_signature(settings.webhook_secret, body, x_hub_signature_256):
            raise HTTPException(401, "bad signature")
        if not x_github_delivery or not x_github_event:
            raise HTTPException(400, "missing X-GitHub-Delivery / X-GitHub-Event")
        try:
            json.loads(body)
        except ValueError:
            raise HTTPException(400, "body is not JSON")

        def _store() -> bool:
            conn = inbox.connect(settings.inbox_path)
            try:
                return inbox.enqueue(conn, x_github_delivery, x_github_event, body.decode())
            finally:
                conn.close()

        is_new = await run_in_threadpool(_store)
        # 202 = "accepted, will process later". A duplicate is still a SUCCESS from the sender's view: answering
        # 4xx/5xx would make it retry the same delivery again and again.
        return JSONResponse({"status": "accepted" if is_new else "duplicate"}, status_code=202 if is_new else 200)

    @app.get("/v1/admin/inbox", tags=["ops"])
    def inbox_stats(_client: str = Depends(guard)):
        conn = inbox.connect(settings.inbox_path)
        try:
            return inbox.stats(conn)
        finally:
            conn.close()

    return app


def app_from_env() -> FastAPI:  # `uvicorn app.main:app_from_env --factory`
    return create_app(Settings.from_env())
