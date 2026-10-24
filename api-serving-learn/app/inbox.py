"""STAGE 3 — the webhook INBOX pattern (this is how reliable webhook consumers are built).

    sender ──POST──▶ [verify signature] ──▶ [INSERT into inbox, dedupe on delivery id] ──▶ 202 (fast!)
                                                         │
                                                  worker, separately:  pending ─▶ processed
                                                                               ─▶ failed (retry) ─▶ dead
                                                                               ─▶ ignored (event we don't handle)

LEARN, the rules senders (GitHub, Stripe, Shopify, ...) force on you:
  * Answer in seconds. Senders time out (GitHub: 10s) and then RETRY -> you must tolerate duplicates.
    So: do the minimum in the request (verify, persist, ack) and process later.
  * Delivery is at-least-once and unordered. So dedupe on the delivery id, and make the apply step
    order-independent (compare updated_at; never let older data overwrite newer).
  * Persist the RAW event before doing anything clever. If your processing has a bug you replay the inbox.
  * Poison messages (permanently bad) must not retry forever -> park as 'dead' and alert on the count.

SQLite stands in for the operational DB you'd really use (Postgres / DynamoDB) or for a queue (SQS, Kafka —
see ../kafka-learn). It is used here for its single-file durability and `INSERT OR IGNORE`.
"""
from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS inbox (
    delivery_id TEXT PRIMARY KEY,           -- the sender's unique id for this delivery: our dedupe key
    event       TEXT NOT NULL,
    received_at TEXT NOT NULL,
    payload     TEXT NOT NULL,              -- raw body, exactly as received
    status      TEXT NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    error       TEXT
);
CREATE INDEX IF NOT EXISTS inbox_status ON inbox(status);
CREATE TABLE IF NOT EXISTS issue_state (    -- the "current state" projection built from events
    id INTEGER PRIMARY KEY, number INTEGER, title TEXT, state TEXT, updated_at TEXT, last_delivery TEXT
);
"""


class PermanentError(Exception):
    """The message can never succeed (bad shape). Don't retry; park it."""


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # readers don't block the writer
    conn.executescript(SCHEMA)
    return conn


def enqueue(conn: sqlite3.Connection, delivery_id: str, event: str, payload: str) -> bool:
    """True if this delivery is new, False if it's a duplicate. INSERT OR IGNORE + the PRIMARY KEY makes the
    dedupe atomic — two concurrent retries of the same delivery can't both win (a SELECT-then-INSERT could)."""
    with conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO inbox (delivery_id, event, received_at, payload) VALUES (?, ?, ?, ?)",
            (delivery_id, event, datetime.now(timezone.utc).isoformat(), payload),
        )
    return cur.rowcount == 1


def _apply(conn: sqlite3.Connection, delivery_id: str, event: str, payload: dict) -> str:
    """Apply one event to the projection. Returns the final inbox status."""
    if event == "ping":
        return "processed"
    if event != "issues":
        return "ignored"  # a valid event we choose not to handle is NOT an error
    issue = payload.get("issue")
    if not isinstance(issue, dict) or not all(k in issue for k in ("id", "number", "title", "state", "updated_at")):
        raise PermanentError("issues event without a usable 'issue' object")
    with conn:
        # LEARN: ">=" on updated_at makes this safe under out-of-order delivery and redelivery (idempotent).
        conn.execute(
            """INSERT INTO issue_state (id, number, title, state, updated_at, last_delivery) VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET number=excluded.number, title=excluded.title, state=excluded.state,
                   updated_at=excluded.updated_at, last_delivery=excluded.last_delivery
               WHERE excluded.updated_at >= issue_state.updated_at""",
            (issue["id"], issue["number"], issue["title"], issue["state"], issue["updated_at"], delivery_id),
        )
    return "processed"


def process_pending(conn: sqlite3.Connection, max_attempts: int = 3) -> Counter:
    stats: Counter = Counter()
    rows = conn.execute(
        "SELECT delivery_id, event, payload, attempts FROM inbox WHERE status IN ('pending','failed') ORDER BY received_at, rowid"
    ).fetchall()
    for r in rows:
        try:
            status = _apply(conn, r["delivery_id"], r["event"], json.loads(r["payload"]))
            error = None
        except PermanentError as e:
            status, error = "dead", str(e)
        except Exception as e:  # transient (db locked, downstream blip): retry later, up to a limit
            status = "dead" if r["attempts"] + 1 >= max_attempts else "failed"
            error = f"{type(e).__name__}: {e}"
        with conn:
            conn.execute("UPDATE inbox SET status=?, attempts=attempts+1, error=? WHERE delivery_id=?",
                         (status, error, r["delivery_id"]))
        stats[status] += 1
    return stats


def stats(conn: sqlite3.Connection) -> dict:
    """What an on-call engineer wants first: how many are stuck, and how old is the oldest pending one."""
    by_status = {r["status"]: r["n"] for r in conn.execute("SELECT status, count(*) AS n FROM inbox GROUP BY status")}
    oldest = conn.execute("SELECT min(received_at) AS t FROM inbox WHERE status IN ('pending','failed')").fetchone()["t"]
    return {"by_status": by_status, "oldest_unprocessed_at": oldest}
