"""A local imitation of GET /repos/{owner}/{repo}/issues with the behaviours that bite in production.

  * default state=open (!), PRs mixed into issues, per_page max 100
  * Link-header pagination, `since` filter (inclusive), sort/direction
  * ETag + If-None-Match -> 304 (not counted against quota)
  * X-RateLimit-* headers; 403 + remaining=0 when the window is exhausted
  * bearer-token auth -> 401
  * fault injection: a scripted queue (tests) and a random rate (demo): 429+Retry-After, 500, 502
  * POST /_admin/mutate[?bad=1] simulates the passage of time: edits, closes, new issues, malformed rows, schema drift

Run:  python -m ghpipe.mock_server --port 8099 --fault-rate 0.15 --rate-limit 40 --window 10
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

LABELS = ["bug", "feature", "docs", "performance", "security", "good first issue", "infra"]
USERS = [(1000 + i, f"user{i}") for i in range(12)]
ISO = "%Y-%m-%dT%H:%M:%SZ"
ROUTE = re.compile(r"^/repos/([^/]+)/([^/]+)/issues$")


def _iso(dt: datetime) -> str:
    return dt.strftime(ISO)


class MockGitHub:
    def __init__(self, n_issues: int = 250, seed: int = 7, token: str | None = "dev-token", rate_limit: int | None = None,
                 window: int = 60, fault_rate: float = 0.0, port: int = 0):
        self.token, self.rate_limit, self.window, self.fault_rate = token, rate_limit, window, fault_rate
        self.faults: list[int] = []  # scripted: next N requests fail with these statuses
        self.request_log: list[str] = []
        self._rnd = random.Random(seed)
        self._lock = threading.Lock()
        self._used, self._reset_at = 0, time.time() + window
        self.issues: list[dict] = []
        self._seed(n_issues)
        self._server = ThreadingHTTPServer(("127.0.0.1", port), _make_handler(self))
        self._server.daemon_threads = True
        self._thread: threading.Thread | None = None

    # -- data -----------------------------------------------------------------------------------
    def _seed(self, n: int) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        for i in range(1, n + 1):
            created = now - timedelta(days=self._rnd.uniform(1, 60))
            closed_at = None
            if self._rnd.random() < 0.6:
                closed_at = min(created + timedelta(hours=self._rnd.uniform(1, 240)), now - timedelta(minutes=self._rnd.uniform(1, 600)))
            updated = closed_at or min(created + timedelta(hours=self._rnd.uniform(0, 48)), now - timedelta(minutes=self._rnd.uniform(1, 600)))
            self.issues.append(self._render(i, created, updated, closed_at, is_pr=self._rnd.random() < 0.2))

    def _render(self, number: int, created: datetime, updated: datetime, closed: datetime | None, *, is_pr: bool) -> dict:
        uid, login = self._rnd.choice(USERS)
        issue = {
            "id": 1_000_000 + number, "number": number, "title": f"Issue #{number}: {self._rnd.choice(['crash on', 'slow', 'wrong total in', 'add'])} {self._rnd.choice(['checkout', 'refunds', 'ledger', 'webhooks'])}",
            "state": "closed" if closed else "open",
            "user": {"id": uid, "login": login, "type": "User", "avatar_url": f"https://example.invalid/{uid}"},
            "labels": [{"id": 5000 + LABELS.index(l), "name": l, "color": "ededed"} for l in self._rnd.sample(LABELS, self._rnd.randint(0, 3))],
            "comments": self._rnd.randint(0, 12), "body": "lorem ipsum " * 5,
            "created_at": _iso(created), "updated_at": _iso(updated), "closed_at": _iso(closed) if closed else None,
        }
        if is_pr:
            issue["pull_request"] = {"url": f"https://example.invalid/pulls/{number}"}
        return issue

    def mutate(self, bad: bool = False) -> None:
        """Time passes: some issues are edited, some closed, new ones arrive."""
        with self._lock:
            latest = max(i["updated_at"] for i in self.issues)
            ts = max(datetime.now(timezone.utc).replace(microsecond=0),
                     datetime.strptime(latest, ISO).replace(tzinfo=timezone.utc) + timedelta(seconds=1))
            open_issues = [i for i in self.issues if i["state"] == "open"]
            for k, i in enumerate(self._rnd.sample(open_issues, min(8, len(open_issues)))):
                i["comments"] += 1
                i["updated_at"] = _iso(ts + timedelta(seconds=k))
                if k < 3:
                    i["state"], i["closed_at"] = "closed", i["updated_at"]
            base = max(i["number"] for i in self.issues)
            now = ts + timedelta(seconds=10)
            for n in range(1, 5):
                self.issues.append(self._render(base + n, now, now, None, is_pr=False))
            if bad:
                base += 5
                broken = self._render(base, now, now, None, is_pr=False)
                broken["title"] = None  # violates the contract
                weird = self._render(base + 1, now, now, None, is_pr=False)
                weird["state"] = "merged"  # unknown enum value
                drifted = self._render(base + 2, now, now, None, is_pr=False)
                drifted["reactions"] = {"+1": 3}  # provider added a field: must NOT break us
                drifted["type"] = {"name": "Bug"}
                self.issues += [broken, weird, drifted]

    # -- lifecycle ------------------------------------------------------------------------------
    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}"

    def start(self) -> "MockGitHub":
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    # -- request handling -----------------------------------------------------------------------
    def _rate_headers(self) -> dict[str, str]:
        if self.rate_limit is None:
            return {}
        return {"X-RateLimit-Limit": str(self.rate_limit),
                "X-RateLimit-Remaining": str(max(self.rate_limit - self._used, 0)),
                "X-RateLimit-Reset": str(math.ceil(self._reset_at))}

    def handle_get(self, h: BaseHTTPRequestHandler) -> None:
        with self._lock:
            self.request_log.append(h.path)
            u = urlparse(h.path)

            if self.token and h.headers.get("Authorization") not in (f"Bearer {self.token}", f"token {self.token}"):
                return _send(h, 401, {"message": "Bad credentials"})

            fault = self.faults.pop(0) if self.faults else None
            if fault is None and self.fault_rate and self._rnd.random() < self.fault_rate:
                fault = self._rnd.choice([429, 500, 502])
            if fault:
                extra = {"Retry-After": "1"} if fault == 429 else {}
                return _send(h, fault, {"message": "injected fault"}, extra)

            m = ROUTE.match(u.path)
            if not m:
                return _send(h, 404, {"message": "Not Found"})

            if self.rate_limit is not None:
                if time.time() >= self._reset_at:
                    self._used, self._reset_at = 0, time.time() + self.window
                self._used += 1
                if self._used > self.rate_limit:
                    return _send(h, 403, {"message": "API rate limit exceeded"}, self._rate_headers())

            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            per_page, page = min(int(q.get("per_page", 30)), 100), max(int(q.get("page", 1)), 1)
            state = q.get("state", "open")  # real default is open-only
            rows = [i for i in self.issues if state == "all" or i["state"] == state]
            if "since" in q:
                rows = [i for i in rows if i["updated_at"] >= q["since"]]
            rows.sort(key=lambda i: (i["updated_at"], i["id"]), reverse=q.get("direction", "desc") == "desc")
            last = max(math.ceil(len(rows) / per_page), 1)
            chunk = rows[(page - 1) * per_page: page * per_page]

            links = []
            for rel, p in (("next", page + 1 if page < last else None), ("last", last)):
                if p:
                    links.append(f'<{self.url}{u.path}?{urlencode({**q, "page": p})}>; rel="{rel}"')
            body = json.dumps(chunk).encode()
            etag = '"' + hashlib.md5(body).hexdigest() + '"'
            headers = {"ETag": etag, **({"Link": ", ".join(links)} if links else {})}
            if h.headers.get("If-None-Match") == etag:
                if self.rate_limit is not None:
                    self._used -= 1  # conditional hits are free
                return _send(h, 304, None, {"ETag": etag, **self._rate_headers()})
            _send(h, 200, chunk, {**headers, **self._rate_headers()})

    def handle_post(self, h: BaseHTTPRequestHandler) -> None:
        u = urlparse(h.path)
        if u.path == "/_admin/mutate":
            self.mutate(bad=parse_qs(u.query).get("bad", ["0"])[0] == "1")
            return _send(h, 200, {"issues": len(self.issues)})
        _send(h, 404, {"message": "Not Found"})


def _send(h: BaseHTTPRequestHandler, status: int, obj, headers: dict | None = None) -> None:
    body = b"" if obj is None else json.dumps(obj).encode()
    h.send_response(status)
    h.send_header("Content-Type", "application/json")
    h.send_header("Content-Length", str(len(body)))
    for k, v in (headers or {}).items():
        h.send_header(k, v)
    h.end_headers()
    h.wfile.write(body)


def _make_handler(app: MockGitHub):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):  # silence
            pass

        def do_GET(self):
            app.handle_get(self)

        def do_POST(self):
            app.handle_post(self)

    return Handler


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8099)
    ap.add_argument("--issues", type=int, default=250)
    ap.add_argument("--token", default="dev-token")
    ap.add_argument("--fault-rate", type=float, default=0.0)
    ap.add_argument("--rate-limit", type=int, default=None)
    ap.add_argument("--window", type=int, default=60)
    a = ap.parse_args()
    srv = MockGitHub(a.issues, token=a.token, rate_limit=a.rate_limit, window=a.window, fault_rate=a.fault_rate, port=a.port)
    print(f"mock GitHub on {srv.url}  (token={a.token!r}, fault_rate={a.fault_rate}, rate_limit={a.rate_limit}/{a.window}s)", flush=True)
    srv._server.serve_forever()


if __name__ == "__main__":
    main()
