# Lesson 7: Idempotency

**Time:** 45 min · **Needs:** Lesson 6 · **Run:** `python api-course/07_idempotency/demo.py` (no server needed)

## The problem

Every reliable pipeline *will* run the same data twice: a retry after a failure, an overlapping incremental window, a manual backfill,
a scheduler that fires a run twice. If running twice changes the answer, your numbers are wrong, and often **nobody notices for weeks**
because the dashboards still render.

## The idea

A load is **idempotent** if running it once or ten times with the same input leaves the same result. That's the property that makes everything else in this
course safe: retries, overlapping windows, duplicate webhook deliveries, replays.

```
plain INSERT:   run 1 -> 3 rows,  run 2 -> 6 rows   (double-counted)
UPSERT by key:  run 1 -> 3 rows,  run 2 -> 3 rows   (idempotent)
```

### What it requires

1. **A stable identity per record:** a *natural key* from the source (the API's `id`) or a deterministic key you construct (hash of the fields that define "the same thing"). Never an auto-increment you generate during the load, since a re-run would create new ids.
2. **Upsert semantics:** "insert, or update if it exists." Spelled `INSERT ... ON CONFLICT DO UPDATE` (Postgres, DuckDB, SQLite), `MERGE INTO` (Snowflake, BigQuery, Databricks, SQL Server), or `INSERT OVERWRITE PARTITION` (Hive/Spark).
3. **An ordering guard:** only let a **newer or equal** version overwrite an older one.

### The ordering guard (the subtle one)

Retries and replays make **old data arrive late**. Part 3 of the demo:

```
plain upsert result : 'open'   at 2026-01-01 10:00   <- WRONG, the stale version overwrote the new one
guarded upsert      : 'closed' at 2026-01-05 09:00   <- right
```

The guard is one clause: `... DO UPDATE SET ... WHERE excluded.updated_at >= issues.updated_at`.
Use `>=`, not `>`: if two real versions carry the same timestamp (1-second precision), the later arrival should win.

### Other ways to get idempotency

| Technique | When |
|---|---|
| **Overwrite a partition** (delete that day, insert that day, in one transaction) | Batch data keyed by date; the standard Spark/Hive/Airflow pattern. Re-running a day replaces that day |
| **De-duplicate on read** (`row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1`) | Warehouses that don't enforce primary keys, or append-only raw tables. Demo part 4 |
| **Idempotency keys** | When *calling* an API that creates things (payments). The server remembers the key and won't repeat the effect |
| **Dedupe table of processed ids** | Event/webhook handling (lesson 11) |

### Idempotent ≠ no side effects

Sending an email isn't idempotent unless you record "already sent". Idempotency is something you design for each external effect.

## Run it

**Predict:** how many rows after 1, 2 and 3 runs of the naive load? Of the upsert load? Which version wins in the out-of-order test?

```
  after attempt 1: 3 rows (should be 3)
  after attempt 2: 6 rows (should be 3)        <- the naive load double-counted
  ...upsert: 3, 3, 3 rows
```

## In real jobs

- "How do you make a pipeline idempotent?" is among the most common data engineering interview questions. The answer is the three requirements above, plus the order-of-operations rule from lesson 6.
- **Test it:** run your pipeline twice on the same input and assert the tables are identical. It's one of the cheapest, highest-value tests you can write (see `test_pipeline_is_idempotent`).
- Cloud warehouses (BigQuery, Snowflake, Redshift) usually *don't enforce* primary keys. The upsert key is a convention your `MERGE` statement relies on, so the discipline is yours.

## Where it is in the repo

`ghpipe/warehouse.py` → `load_silver()` (the `ON CONFLICT ... WHERE excluded.updated_at >= ...` upsert, plus in-batch dedupe);
tests `test_pipeline_is_idempotent`, `test_stale_data_never_overwrites_newer`.

## Try it yourself

1. Remove the `PRIMARY KEY` from the `issues` table in the demo and run the upsert load. What happens, and why?
2. Change `>=` to `>` in the guard and replay the same record with the same timestamp but a changed title. Which title survives?
3. Rewrite the guarded upsert as a `MERGE` statement (look up the syntax for Snowflake or BigQuery).

## Check yourself

1. Define idempotency in one sentence, then give two situations in a pipeline that rely on it.
2. Why is a surrogate id generated at load time a poor merge key?
3. What bug does the `updated_at >=` guard prevent that a plain upsert doesn't?
4. How would you make a daily batch job over a partitioned table idempotent without an upsert?

<details><summary>Answers</summary>

1. Running an operation repeatedly with the same input produces the same result as running it once. Situations: task retries after partial failure; overlapping incremental windows; backfills; duplicate webhook deliveries.
2. A re-run generates different ids, so the same source record is inserted again instead of matched.
3. Stale data arriving after fresh data (retries, replays, out-of-order delivery) overwriting the newer version.
4. Overwrite that day's partition atomically (delete+insert in one transaction, or `INSERT OVERWRITE`), so a re-run replaces rather than appends.
</details>

**Next:** [Lesson 8: ETags](../08_etag/README.md)
