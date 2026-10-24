"""Airflow 2.x TaskFlow version of the same pipeline.

ILLUSTRATIVE: Airflow is not installed in this repo and this file is not exercised by the tests. It shows how the
four stage functions in ghpipe.pipeline map 1:1 onto tasks. In production the warehouse would be
Snowflake/BigQuery and bronze would live on S3/GCS; the structure does not change.

Design points worth being able to defend in an interview:
  * max_active_runs=1      -> two overlapping runs would race on the same watermark
  * catchup=False          -> this is a "sync current state" job, not a per-interval backfill
  * retries on tasks      -> Airflow retries the *task*; the client's own retries handle single requests
  * RateLimitExceeded      -> raised (not slept on); Airflow's retry_delay is the cheap way to wait out a quota
  * tokens in a Connection -> never in code or the DAG file
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

from ghpipe import pipeline, quality, warehouse
from ghpipe.client import ApiClient
from ghpipe.config import Settings


def _settings() -> Settings:
    conn = BaseHook.get_connection("github_api")  # host + password configured in the Airflow UI / secrets backend
    return Settings(base_url=conn.host, token=conn.password, repo=conn.extra_dejson["repo"])


@dag(
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True},
    tags=["ingestion", "github"],
)
def github_issues():
    @task
    def extract(run_id: str | None = None) -> dict:
        s = _settings()
        con = warehouse.connect(s.db_path)
        rid = run_id or datetime.utcnow().strftime("%Y%m%d%H%M%S")
        warehouse.start_run(con, rid, s.source_key)
        client = ApiClient(s.base_url, s.token)
        ex = pipeline.extract(s, client, con, rid)
        return {**ex.__dict__, "files": [str(f) for f in ex.files]}  # XCom must be JSON: pass pointers, never data

    @task
    def load(ex: dict) -> dict:
        s = _settings()
        if ex["not_modified"]:
            return {**ex, "stats": {}}
        con = warehouse.connect(s.db_path)
        from pathlib import Path

        before = con.execute("SELECT count(*) FROM silver.issues").fetchall()[0][0]
        stats = warehouse.load_silver(con, [Path(f) for f in ex["files"]])
        return {**ex, "stats": dict(stats), "rows_before": before}

    @task
    def check(ld: dict) -> dict:
        if ld["not_modified"]:
            return ld
        s = _settings()
        con = warehouse.connect(s.db_path)
        checks = quality.run_checks(con, fetched=ld["fetched"], rejected=ld["stats"].get("rejected", 0),
                                    rows_before=ld["rows_before"], max_reject_rate=s.max_reject_rate)
        quality.persist(con, ld["run_id"], checks)
        quality.enforce(checks)  # raises -> task fails -> alert; watermark untouched
        return ld

    @task
    def publish(ld: dict) -> None:
        s = _settings()
        con = warehouse.connect(s.db_path)
        if not ld["not_modified"]:
            pipeline.publish(s, con, pipeline.ExtractResult(
                run_id=ld["run_id"], max_updated_at=ld["max_updated_at"], etag=ld["etag"], request_key=ld["request_key"]))
        warehouse.finish_run(con, ld["run_id"], "no_change" if ld["not_modified"] else "success", fetched=ld["fetched"])

    publish(check(load(extract())))


github_issues()
