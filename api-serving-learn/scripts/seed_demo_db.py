"""Create a small warehouse with the same silver.issues shape part 1 produces, so this project runs standalone.

    python scripts/seed_demo_db.py            # -> data/warehouse.duckdb
Or skip this and serve part 1's real output:  DB_PATH=../api-ingestion-learn/data/warehouse.duckdb make serve
"""
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

DDL = """CREATE SCHEMA IF NOT EXISTS silver;
CREATE OR REPLACE TABLE silver.issues (
    id BIGINT PRIMARY KEY, repo VARCHAR, number INTEGER, title VARCHAR, state VARCHAR, author_id BIGINT,
    author_login VARCHAR, labels VARCHAR[], comments INTEGER, is_pull_request BOOLEAN,
    created_at TIMESTAMP, updated_at TIMESTAMP, closed_at TIMESTAMP);"""


def seed(path: Path, n: int = 500, seed_: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rnd = random.Random(seed_)
    now = datetime(2026, 10, 1).replace(microsecond=0)
    con = duckdb.connect(str(path))
    con.execute(DDL)
    rows = []
    for i in range(1, n + 1):
        created = now - timedelta(days=rnd.uniform(1, 60))
        closed = created + timedelta(hours=rnd.uniform(1, 200)) if rnd.random() < 0.6 else None
        # deliberately coarse timestamps (whole hours) so MANY rows tie on updated_at: the cursor tie-breaker matters
        updated = (closed or created).replace(minute=0, second=0, microsecond=0)
        rows.append([1_000_000 + i, "acme/payments", i, f"Issue {i}", "closed" if closed else "open", 1000 + i % 12,
                     f"user{i % 12}", rnd.sample(["bug", "feature", "docs", "infra"], rnd.randint(0, 2)),
                     rnd.randint(0, 9), rnd.random() < 0.2, created, updated, closed])
    con.executemany("INSERT INTO silver.issues VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.close()


if __name__ == "__main__":
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "data/warehouse.duckdb")
    seed(target)
    print(f"seeded {target}")
