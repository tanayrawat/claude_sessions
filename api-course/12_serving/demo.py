"""Lesson 12: being the API server (cursor pagination, auth, rate limits). Run:  python api-course/12_serving/demo.py"""
import base64
import dataclasses
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import say, step, title, use_serving_project

use_serving_project()
import duckdb
from fastapi.testclient import TestClient

from app.main import create_app
from app.settings import Settings, hash_key
from scripts.seed_demo_db import seed

tmp = Path(tempfile.mkdtemp())
seed(tmp / "wh.duckdb", n=120)
KEY = {"X-API-Key": "k1"}


class Clock:
    t = 0.0

    def __call__(self):
        return self.t


clock = Clock()
base = Settings(db_path=tmp / "wh.duckdb", inbox_path=tmp / "inbox.sqlite", api_key_hashes={hash_key("k1"): "analytics"},
                rate_capacity=1000, rate_refill_per_sec=1000)
client = TestClient(create_app(base, clock))

title("Lesson 12 - Now YOU are the API: serving data safely")

step("1. OFFSET pagination (page=N) vs CURSOR pagination, while a new row arrives between two requests")


def sql(statement):
    """Open, run, close. DuckDB lets one process hold only one kind of connection to a file at a time."""
    con = duckdb.connect(str(tmp / "wh.duckdb"))
    try:
        return con.execute(statement).fetchall()
    finally:
        con.close()


q = "SELECT id FROM silver.issues WHERE NOT is_pull_request ORDER BY updated_at DESC, id DESC"
page1_off = [r[0] for r in sql(q + " LIMIT 10 OFFSET 0")]
first = client.get("/v1/issues", headers=KEY, params={"limit": 10}).json()
sql("""INSERT INTO silver.issues VALUES (9999999, 'acme/payments', 999, 'brand new', 'open', 1, 'u', [], 0,
       false, TIMESTAMP '2030-01-01', TIMESTAMP '2030-01-01', NULL)""")  # a fresh row lands at the top
page2_off = [r[0] for r in sql(q + " LIMIT 10 OFFSET 10")]
second = client.get("/v1/issues", headers=KEY, params={"limit": 10, "cursor": first["next_cursor"]}).json()
dup_off = set(page1_off) & set(page2_off)
dup_cur = {i["id"] for i in first["data"]} & {i["id"] for i in second["data"]}
print(f"  OFFSET: page 2 repeats {len(dup_off)} row(s) from page 1  {sorted(dup_off)}")
print(f"  CURSOR: page 2 repeats {len(dup_cur)} row(s)")
decoded = base64.urlsafe_b64decode(first["next_cursor"] + "==").decode()
print(f"  the cursor is just the last row's sort key: {decoded}")
say("OFFSET means 'skip N rows', so when rows are inserted above you, everything shifts. A cursor says 'continue after THIS "
    "row', which stays true no matter what else changes. It is also faster: the database seeks to the key instead of "
    "counting and discarding N rows.")

step("2. Authentication: who is calling?")
print("  no key       ->", client.get("/v1/issues").status_code)
print("  wrong key    ->", client.get("/v1/issues", headers={"X-API-Key": "nope"}).status_code)
print("  correct key  ->", client.get("/v1/issues", headers=KEY).status_code)
print("  /health      ->", client.get("/health").status_code, "(probes and load balancers can't send keys)")

step("3. Rate limiting: a token bucket per client (capacity 3, refills 1 token per second)")
limited = TestClient(create_app(dataclasses.replace(base, rate_capacity=3, rate_refill_per_sec=1.0,
                                                    api_key_hashes={hash_key("k1"): "analytics", hash_key("k2"): "bi"}), clock))
for i in range(1, 6):
    r = limited.get("/v1/issues", headers=KEY, params={"limit": 1})
    extra = f"  Retry-After={r.headers['retry-after']}s" if r.status_code == 429 else ""
    print(f"  analytics request {i}: {r.status_code}  remaining={r.headers.get('x-ratelimit-remaining')}{extra}")
print("  bi (another client) :", limited.get("/v1/issues", headers={"X-API-Key": "k2"}, params={"limit": 1}).status_code, "<- its own bucket")
clock.t += 1.0
print("  analytics, 1s later :", limited.get("/v1/issues", headers=KEY, params={"limit": 1}).status_code, "<- one token refilled")
say("This is the server side of lesson 5. These are the exact headers your client code read there.")

step("4. ETag: the server side of lesson 8")
r1 = client.get("/v1/issues", headers=KEY, params={"limit": 5})
r2 = client.get("/v1/issues", headers={**KEY, "If-None-Match": r1.headers["etag"]}, params={"limit": 5})
print(f"  first call {r1.status_code} ({len(r1.content)} bytes); repeat with If-None-Match -> {r2.status_code} ({len(r2.content)} bytes)")
