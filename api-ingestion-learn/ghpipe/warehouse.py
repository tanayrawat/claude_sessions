"""DuckDB as a stand-in for Snowflake/BigQuery/Redshift: schemas, state, run log, silver upsert."""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from .models import Issue, utc_naive

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

SILVER_SCHEMA = pa.schema(
    [
        ("id", pa.int64()), ("repo", pa.string()), ("number", pa.int32()), ("title", pa.string()),
        ("state", pa.string()), ("author_id", pa.int64()), ("author_login", pa.string()),
        ("labels", pa.list_(pa.string())), ("comments", pa.int32()), ("is_pull_request", pa.bool_()),
        ("created_at", pa.timestamp("us")), ("updated_at", pa.timestamp("us")), ("closed_at", pa.timestamp("us")),
        ("_run_id", pa.string()), ("_loaded_at", pa.timestamp("us")),
    ]
)


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute((SQL_DIR / "ddl.sql").read_text())
    return con


def refresh_gold(con: duckdb.DuckDBPyConnection) -> None:
    con.execute((SQL_DIR / "gold.sql").read_text())


# -- incremental state ------------------------------------------------------------------------
def get_state(con, source: str) -> dict:
    rows = con.execute("SELECT watermark, etag_key, etag FROM ops.pipeline_state WHERE source = ?", [source]).fetchall()
    return dict(zip(("watermark", "etag_key", "etag"), rows[0])) if rows else {"watermark": None, "etag_key": None, "etag": None}


def set_state(con, source: str, watermark: str | None, etag_key: str, etag: str | None) -> None:
    con.execute(
        """INSERT INTO ops.pipeline_state (source, watermark, etag_key, etag) VALUES (?, ?, ?, ?)
           ON CONFLICT (source) DO UPDATE SET watermark = excluded.watermark, etag_key = excluded.etag_key,
                                              etag = excluded.etag, updated_at = now()""",
        [source, watermark, etag_key, etag],
    )


# -- run log ----------------------------------------------------------------------------------
def start_run(con, run_id: str, source: str) -> None:
    con.execute(
        "INSERT INTO ops.pipeline_runs (run_id, source, started_at, status) VALUES (?, ?, ?, 'running')",
        [run_id, source, utc_naive(datetime.now(timezone.utc))],
    )


def finish_run(con, run_id: str, status: str, **m) -> None:
    con.execute(
        """UPDATE ops.pipeline_runs SET finished_at = ?, status = ?, rows_fetched = ?, rows_inserted = ?,
               rows_updated = ?, rows_rejected = ?, http_requests = ?, http_retries = ?, error = ? WHERE run_id = ?""",
        [utc_naive(datetime.now(timezone.utc)), status, m.get("fetched"), m.get("inserted"), m.get("updated"),
         m.get("rejected"), m.get("requests"), m.get("retries"), m.get("error"), run_id],
    )


# -- silver -----------------------------------------------------------------------------------
def _to_row(issue: Issue, source: str, run_id: str, loaded_at: datetime) -> dict:
    return {
        "id": issue.id, "repo": source, "number": issue.number, "title": issue.title, "state": issue.state,
        "author_id": issue.user.id if issue.user else None, "author_login": issue.user.login if issue.user else None,
        "labels": [l.name for l in issue.labels], "comments": issue.comments,
        "is_pull_request": issue.pull_request is not None,
        "created_at": utc_naive(issue.created_at), "updated_at": utc_naive(issue.updated_at),
        "closed_at": utc_naive(issue.closed_at) if issue.closed_at else None,
        "_run_id": run_id, "_loaded_at": loaded_at,
    }


def load_silver(con, files: list[Path]) -> Counter:
    """Validate bronze records, quarantine bad ones, upsert good ones. Safe to run twice on the same files."""
    loaded_at = utc_naive(datetime.now(timezone.utc))
    good: dict[int, dict] = {}
    rejects: list[dict] = []
    stats: Counter = Counter()

    for f in files:
        for rec in pq.read_table(f).to_pylist():
            stats["fetched"] += 1
            try:
                issue = Issue.model_validate(json.loads(rec["payload"]))
            except ValueError as e:  # pydantic.ValidationError and JSONDecodeError are both ValueErrors
                if isinstance(e, ValidationError):
                    first = e.errors()[0]
                    err = f"{e.error_count()} validation error(s); first: {'.'.join(map(str, first['loc']))} {first['msg']}"
                else:
                    err = f"{type(e).__name__}: {e}"
                rejects.append({"run_id": rec["run_id"], "source": rec["source"], "payload": rec["payload"],
                                "error": err, "rejected_at": loaded_at})
                continue
            row = _to_row(issue, rec["source"], rec["run_id"], loaded_at)
            prev = good.get(row["id"])
            # same record can appear twice in one run (pages shift while we paginate): keep the freshest
            if prev is None or row["updated_at"] >= prev["updated_at"]:
                good[row["id"]] = row

    stats["rejected"] = len(rejects)
    if rejects:
        con.executemany(
            "INSERT INTO silver.dead_letter VALUES (?, ?, ?, ?, ?)",
            [[r["run_id"], r["source"], r["payload"], r["error"], r["rejected_at"]] for r in rejects],
        )
    if good:
        con.register("batch", pa.Table.from_pylist(list(good.values()), schema=SILVER_SCHEMA))
        stats["inserted"], stats["updated"] = con.execute(
            """SELECT count(*) FILTER (WHERE s.id IS NULL), count(*) FILTER (WHERE b.updated_at > s.updated_at)
               FROM batch b LEFT JOIN silver.issues s ON b.id = s.id"""
        ).fetchall()[0]
        # >= not >: providers stamp updated_at at 1s resolution, so two real edits can share a timestamp;
        # last-arrival wins on ties, strictly older (out-of-order/replayed) data can never overwrite newer.
        con.execute(
            """INSERT INTO silver.issues SELECT * FROM batch
               ON CONFLICT (id) DO UPDATE SET
                   repo = excluded.repo, number = excluded.number, title = excluded.title, state = excluded.state,
                   author_id = excluded.author_id, author_login = excluded.author_login, labels = excluded.labels,
                   comments = excluded.comments, is_pull_request = excluded.is_pull_request,
                   created_at = excluded.created_at, updated_at = excluded.updated_at, closed_at = excluded.closed_at,
                   _run_id = excluded._run_id, _loaded_at = excluded._loaded_at
               WHERE excluded.updated_at >= silver.issues.updated_at"""
        )
        con.unregister("batch")
    return stats


def rebuild_silver(con, files: list[Path]) -> Counter:
    """Replay: throw silver away and rebuild it purely from bronze. No API calls."""
    con.execute("DELETE FROM silver.issues")
    con.execute("DELETE FROM silver.dead_letter")
    stats = load_silver(con, files)
    refresh_gold(con)
    return stats
