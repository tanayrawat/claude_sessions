from __future__ import annotations

import argparse
import logging

from . import bronze, warehouse
from .client import ApiClient
from .config import Settings
from .pipeline import run_pipeline


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="ghpipe")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run", help="incremental extract -> bronze -> silver -> DQ -> gold")
    sub.add_parser("replay", help="rebuild silver+gold from bronze only (no API calls)")
    sub.add_parser("runs", help="show run history")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # per-request INFO lines drown the signal
    s = Settings.from_env()

    if args.cmd == "run":
        client = ApiClient(s.base_url, s.token)
        try:
            r = run_pipeline(s, client)
        finally:
            client.close()
        print(r)
    elif args.cmd == "replay":
        con = warehouse.connect(s.db_path)
        print(dict(warehouse.rebuild_silver(con, bronze.all_files(s.bronze_dir))))
    elif args.cmd == "runs":
        con = warehouse.connect(s.db_path)
        rows = con.execute(
            """SELECT run_id, status, rows_fetched, rows_inserted, rows_updated, rows_rejected,
                      http_requests, http_retries, left(coalesce(error, ''), 60)
               FROM ops.pipeline_runs ORDER BY started_at"""
        ).fetchall()
        print(f"{'run_id':<13}{'status':<10}{'fetch':>6}{'ins':>5}{'upd':>5}{'rej':>5}{'http':>6}{'retry':>6}  error")
        for r in rows:
            print(f"{r[0]:<13}{r[1]:<10}{r[2] or 0:>6}{r[3] or 0:>5}{r[4] or 0:>5}{r[5] or 0:>5}{r[6] or 0:>6}{r[7] or 0:>6}  {r[8]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
