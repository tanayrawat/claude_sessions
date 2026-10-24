"""Bronze layer: land the API response *as received*, immutable, partitioned by ingest date.

Why: if silver logic is wrong (or we later want a new field), we replay from bronze instead of
re-calling a rate-limited API that may no longer return the old state.
One row per record; `payload` is the untouched JSON.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("source", pa.string()),
        ("page_no", pa.int32()),
        ("request_url", pa.string()),
        ("http_status", pa.int32()),
        ("fetched_at", pa.timestamp("us", tz="UTC")),
        ("payload", pa.string()),
    ]
)


def write_page(bronze_dir: Path, *, run_id: str, source: str, page_no: int, url: str, status: int, records: list) -> Path:
    now = datetime.now(timezone.utc)
    part = bronze_dir / "issues" / f"ingest_date={now:%Y-%m-%d}" / f"run_id={run_id}"
    part.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "run_id": [run_id] * len(records),
            "source": [source] * len(records),
            "page_no": [page_no] * len(records),
            "request_url": [url] * len(records),
            "http_status": [status] * len(records),
            "fetched_at": [now] * len(records),
            "payload": [json.dumps(r) for r in records],
        },
        schema=SCHEMA,
    )
    final = part / f"page-{page_no:05d}.parquet"
    tmp = final.with_suffix(".tmp")
    pq.write_table(table, tmp)
    os.replace(tmp, final)  # atomic: readers never see a half-written file
    return final


def all_files(bronze_dir: Path) -> list[Path]:
    return sorted(bronze_dir.glob("issues/ingest_date=*/run_id=*/page-*.parquet"))
