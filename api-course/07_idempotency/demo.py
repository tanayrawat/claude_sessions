"""Lesson 7: idempotent loads. Run:  python api-course/07_idempotency/demo.py   (uses an in-memory DuckDB, no server)"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import duckdb

from common import say, step, title

title("Lesson 7 - Idempotency: running it twice must equal running it once")

batch = [(1, "Login broken", "open", "2026-01-01 10:00:00"),
         (2, "Slow export", "open", "2026-01-01 11:00:00"),
         (3, "Typo in docs", "closed", "2026-01-01 12:00:00")]


def count(con, table):
    return con.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]


step("1. The naive load: plain INSERT. The job fails after loading, the scheduler retries it")
con = duckdb.connect()
con.execute("CREATE TABLE issues_naive (id INT, title VARCHAR, state VARCHAR, updated_at TIMESTAMP)")
for attempt in (1, 2):
    con.executemany("INSERT INTO issues_naive VALUES (?, ?, ?, ?)", batch)
    print(f"  after attempt {attempt}: {count(con, 'issues_naive')} rows (should be 3)")
say("Every retry, overlap or backfill multiplies the data. Dashboards double-count and nobody notices for weeks.")

step("2. Fix A: give rows an identity (primary key) and UPSERT: insert, or update if the id exists")
con.execute("CREATE TABLE issues (id INT PRIMARY KEY, title VARCHAR, state VARCHAR, updated_at TIMESTAMP)")
UPSERT = """INSERT INTO issues VALUES (?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET title = excluded.title, state = excluded.state, updated_at = excluded.updated_at"""
for attempt in (1, 2, 3):
    con.executemany(UPSERT, batch)
    print(f"  after attempt {attempt}: {count(con, 'issues')} rows")
say("Same input, same result, however many times it runs. That property is called idempotency.")

step("3. A subtler bug: an OLD version of a row arrives AFTER the new one (retries and replays do this)")
newer = (1, "Login broken", "closed", "2026-01-05 09:00:00")
older = (1, "Login broken", "open", "2026-01-01 10:00:00")
con.executemany(UPSERT, [newer])
con.executemany(UPSERT, [older])  # stale data shows up late
state = con.execute("SELECT state, updated_at FROM issues WHERE id = 1").fetchall()[0]
print(f"  plain upsert result : {state[0]!r} at {state[1]}   <- WRONG, the old version won")

GUARDED = UPSERT + " WHERE excluded.updated_at >= issues.updated_at"
con.executemany(GUARDED, [newer])
con.executemany(GUARDED, [older])
state = con.execute("SELECT state, updated_at FROM issues WHERE id = 1").fetchall()[0]
print(f"  guarded upsert      : {state[0]!r} at {state[1]}   <- right: only a newer-or-equal version may overwrite")

step("4. No primary keys (many cloud warehouses don't enforce them)? De-duplicate on read instead")
con.execute("INSERT INTO issues_naive VALUES (1, 'Login broken', 'closed', '2026-01-05 09:00:00')")  # a newer version, also appended
rows = con.execute("""
    SELECT id, state, updated_at FROM (
        SELECT *, row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn FROM issues_naive
    ) WHERE rn = 1 ORDER BY id""").fetchall()
for r in rows:
    print(f"  {r}")
say("'Keep the newest row per id' with row_number() is the standard pattern in Snowflake / BigQuery / Spark. "
    "On big tables you would use MERGE INTO instead, which is the same upsert idea.")
