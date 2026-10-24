"""STAGE 1 — keyset ("cursor") pagination.

LEARN: why not ?page=3&per_page=50 (OFFSET)?
  1. Correctness: if a row is inserted at the top while a client pages, every later page shifts by one ->
     the client sees a duplicate (or, with deletes, skips a row). Part 1 had to design around exactly this.
  2. Cost: OFFSET 100000 makes the database read and throw away 100000 rows. Keyset is "WHERE (a,b) < (x,y)
     LIMIT n": an index seek, same cost on page 1 and page 10,000.

The cursor is the sort key of the last row you returned: (updated_at, id). `id` is the tie-breaker: many rows
share an updated_at, and without a unique tie-breaker you would skip or repeat rows at page boundaries.
The cursor is opaque (base64) so we can change its internals without breaking clients.
"""
from __future__ import annotations

import base64
import json
from datetime import datetime


class BadCursor(ValueError):
    pass


def encode(updated_at: datetime, id_: int) -> str:
    raw = json.dumps({"u": updated_at.isoformat(), "i": id_}).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode(cursor: str) -> tuple[str, int]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        d = json.loads(raw)
        datetime.fromisoformat(d["u"])  # validate shape; the value itself is passed as a bound parameter
        return d["u"], int(d["i"])
    except Exception as e:  # any garbage -> a clean 400, never a 500
        raise BadCursor("invalid cursor") from e
