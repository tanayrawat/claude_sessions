"""Orchestration of one incremental run.

    extract  -> bronze parquet files          (touches the API)
    load     -> silver upsert + dead letter   (pure function of bronze)
    check    -> data-quality gate
    publish  -> gold refresh + advance watermark, in ONE transaction

Each stage is a separate function so an orchestrator (see dags/) can map them 1:1 onto tasks and retry
them independently. The watermark moves last: any failure before it just means "re-read the window".
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlencode

from . import bronze, quality, warehouse
from .client import ApiClient
from .config import Settings

log = logging.getLogger("ghpipe.pipeline")


@dataclass
class ExtractResult:
    run_id: str
    files: list[Path] = field(default_factory=list)
    fetched: int = 0
    max_updated_at: str | None = None
    etag: str | None = None
    request_key: str = ""
    not_modified: bool = False


@dataclass
class RunResult:
    run_id: str
    status: str
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    rejected: int = 0
    requests: int = 0
    retries: int = 0
    watermark: str | None = None


def _iso_minus(ts: str, seconds: int) -> str:
    from datetime import datetime, timedelta, timezone

    dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) - timedelta(seconds=seconds)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def extract(settings: Settings, client: ApiClient, con, run_id: str) -> ExtractResult:
    state = warehouse.get_state(con, settings.source_key)
    path = f"/repos/{settings.repo}/issues"
    # state=all: the API default is open-only, so a "complete" load would silently miss every closed issue.
    # direction=desc: if rows shift while we paginate we get duplicates (harmless: upsert) not gaps (data loss).
    params = {"state": "all", "sort": "updated", "direction": "desc", "per_page": settings.per_page}
    if state["watermark"]:
        params["since"] = _iso_minus(state["watermark"], settings.lookback_seconds)

    key = f"{path}?{urlencode(sorted(params.items()))}"
    # An ETag is only valid for the exact request it came from. Same watermark => same URL => cheap 304 poll.
    etag = state["etag"] if state["etag_key"] == key else None

    result = ExtractResult(run_id=run_id, request_key=key)
    for page_no, resp in enumerate(client.paginate(path, params, etag=etag), start=1):
        if resp.not_modified:
            result.not_modified = True
            return result
        if not isinstance(resp.json, list):
            raise ValueError(f"unexpected response shape on page {page_no}: {type(resp.json).__name__}")
        if page_no == 1:
            result.etag = resp.etag
        if resp.json:
            result.files.append(bronze.write_page(
                settings.bronze_dir, run_id=run_id, source=settings.repo, page_no=page_no,
                url=resp.url, status=resp.status, records=resp.json))
        result.fetched += len(resp.json)
        for rec in resp.json:
            ts = rec.get("updated_at") if isinstance(rec, dict) else None
            if isinstance(ts, str) and (result.max_updated_at is None or ts > result.max_updated_at):
                result.max_updated_at = ts  # ISO-8601 Z strings sort chronologically
    return result


def publish(settings: Settings, con, ex: ExtractResult) -> None:
    con.begin()
    try:
        warehouse.refresh_gold(con)
        prev = warehouse.get_state(con, settings.source_key)["watermark"]
        # never move the watermark backwards (e.g. an empty or replayed window)
        wm = max(filter(None, [prev, ex.max_updated_at]), default=None)
        warehouse.set_state(con, settings.source_key, wm, ex.request_key, ex.etag)
        con.commit()
    except Exception:
        con.rollback()
        raise


def run_pipeline(settings: Settings, client: ApiClient) -> RunResult:
    con = warehouse.connect(settings.db_path)
    run_id = uuid.uuid4().hex[:12]
    warehouse.start_run(con, run_id, settings.source_key)
    req0, retry0 = client.stats["requests"], client.stats["retries"]
    res = RunResult(run_id=run_id, status="failed")
    try:
        ex = extract(settings, client, con, run_id)
        res.fetched = ex.fetched
        if ex.not_modified:
            res.status = "no_change"
            res.watermark = warehouse.get_state(con, settings.source_key)["watermark"]
            return res

        rows_before = con.execute("SELECT count(*) FROM silver.issues").fetchall()[0][0]
        stats = warehouse.load_silver(con, ex.files)
        res.inserted, res.updated, res.rejected = stats["inserted"], stats["updated"], stats["rejected"]

        checks = quality.run_checks(con, fetched=ex.fetched, rejected=res.rejected,
                                    rows_before=rows_before, max_reject_rate=settings.max_reject_rate)
        quality.persist(con, run_id, checks)
        for c in checks:
            if not c.passed:
                log.warning("DQ %s %s: %s", c.severity, c.name, c.detail)
        quality.enforce(checks)

        publish(settings, con, ex)
        res.status = "success"
        res.watermark = warehouse.get_state(con, settings.source_key)["watermark"]
        return res
    except Exception as e:
        warehouse.finish_run(con, run_id, "failed", error=f"{type(e).__name__}: {e}"[:500],
                             fetched=res.fetched, rejected=res.rejected,
                             requests=client.stats["requests"] - req0, retries=client.stats["retries"] - retry0)
        raise
    finally:
        res.requests, res.retries = client.stats["requests"] - req0, client.stats["retries"] - retry0
        if res.status != "failed":
            warehouse.finish_run(con, run_id, res.status, fetched=res.fetched, inserted=res.inserted,
                                 updated=res.updated, rejected=res.rejected,
                                 requests=res.requests, retries=res.retries)
        con.close()
