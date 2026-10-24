"""Inbox worker: drains pending webhook events. Run separately from the web process (`make worker`).

    python -m app.worker           # poll forever
    python -m app.worker --once    # drain once and exit (handy for learning and for cron)
"""
from __future__ import annotations

import argparse
import time

from . import inbox
from .settings import Settings


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args()
    s = Settings.from_env()
    conn = inbox.connect(s.inbox_path)
    while True:
        result = inbox.process_pending(conn, s.max_attempts)
        if result:
            print(dict(result), "|", inbox.stats(conn)["by_status"], flush=True)
        if a.once:
            return
        time.sleep(1)


if __name__ == "__main__":
    main()
