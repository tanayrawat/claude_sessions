# Lesson 10: The whole pipeline

**Time:** 90 min · **Needs:** Lessons 1-9 · **Run:** `python api-course/10_capstone_pipeline/demo.py`

## Why this lesson exists

Lessons 3-9 each taught one defence in isolation. A real pipeline needs all of them, in the right **order**, and the order is where bugs hide.
This lesson shows them working together as a story: one week of an ingestion job, including the bad day. Then you read the real code,
which is no longer a wall of mysterious functions, since you've seen every part fail on its own.

## The pipeline in one picture

```
 ┌──────────┐   ┌─────────────┐   ┌─────────────┐   ┌────────────┐   ┌──────────────┐   ┌────────┐
 │ 1 EXTRACT │──▶│ 2 LAND RAW  │──▶│ 3 VALIDATE  │──▶│ 4 UPSERT   │──▶│ 5 QUALITY    │──▶│ 6 PUBLISH│
 │ paginate  │   │ bronze      │   │ contract →  │   │ into silver│   │ GATE         │   │ gold +   │
 │ retry     │   │ Parquet     │   │ dead letter │   │ idempotent │   │ block on fail│   │ watermark│
 │ rate-limit│   │ (lesson 9)  │   │ (lesson 9)  │   │ (lesson 7) │   │ (lesson 9)   │   │ LAST (6) │
 │ since/ETag│   └─────────────┘   └─────────────┘   └────────────┘   └──────────────┘   └────────┘
 │ (3,4,5,6,8)│                                                              │ fail
 └──────────┘                                                                ▼
                                                    watermark does NOT move; next run re-reads the window
```

Each box is a separate function in `ghpipe/pipeline.py` (`extract`, `load_silver`, `quality.run_checks`, `publish`), so an orchestrator like Airflow can run them
as separate tasks, retry each independently and show which stage failed.

## Run it

```bash
python api-course/10_capstone_pipeline/demo.py
```

### The story, line by line

```
  Mon 09:00  first run (full load)             -> success   fetched=250  inserted=250 ...  http=3
```
Cold start: no watermark, so everything is fetched (3 pages). Lessons 3 and 6.

```
  Mon 09:15  nothing changed yet               -> success   fetched=1    inserted=0   ...  http=1
  Mon 09:30  nothing changed (now a cheap 304) -> no_change fetched=0    ...              http=1
```
The 09:15 run is the first *incremental* run, whose URL contains `since=`, so it can't reuse the full load's ETag (lesson 8, trap 1). It re-reads the one boundary row (idempotent, so harmless).
By 09:30 an ETag exists, and the answer is a free `304`: status `no_change`, nothing to process.

```
    [client log] retryable failure (502) ...; attempt 1, sleeping 0.02s
    [client log] retryable failure (429) ...; attempt 2, sleeping 1.00s
  Tue 09:00  8 edits + 4 new, API hiccups twice -> success   fetched=13   inserted=4    updated=8  ...  http=3
```
Two transient failures absorbed by the client (lesson 4). Only 13 rows moved (lesson 6); the rest is classified exactly: 4 new, 8 updated.
`http=3` counts the two failed attempts plus the one that worked.

```
    [client log] DQ error reject_rate: 2/19 = 10.5% (max 5%)
  Wed 09:00  source sends 2 malformed records  -> BLOCKED by data-quality gate
  Wed 09:30  operator accepts up to 50% rejects -> success   fetched=19   rejected=2
```
2 of 19 records break the contract: 10.5% exceeds the 5% threshold, so the gate **blocks the run and the watermark stays put**. An engineer investigates, decides these are acceptable, and raises
the threshold. The next run re-reads the same window (safe: idempotent) and succeeds. In real life you would fix the cause or contact the provider.

```
   ('failed', 19, None, None, 2, 1, 0, 'DataQualityError: reject_rate: 2/19 = 10.5% (max')
```
The run log is the first place to look when "the dashboard is stale". Every run recorded its status, volumes, HTTP requests and retries.

```
   1 validation error(s); first: state Input should be 'open' or 'closed'   (x2)
   1 validation error(s); first: title Input should be a valid string        (x2)
```
Four dead-letter rows for two bad records: the blocked run recorded them and the accepted rerun recorded the same two again (it re-read the window). In production you'd de-duplicate dead letters by payload hash; here the repetition is a faithful look at what "re-read the window" means.

```
  silver rows before: 259, after replay: 259  (rebuilt from 7 saved files)
```
Friday: the source system is shut down (the demo stops the server), and silver is rebuilt entirely from the saved raw files, with **zero API calls**. Lesson 9's insurance, paid out.

## Now read the real code (in this order, about 45 minutes)

Open `api-ingestion-learn/ghpipe/` and read these with the diagram above beside you. For each function, name the lesson it implements.

1. `pipeline.py` → `run_pipeline()`: the order of the six stages. Find where the watermark moves and why it's last.
2. `pipeline.py` → `extract()`: the query parameters (`state=all`, `direction=desc`, `since`) and the ETag bookkeeping.
3. `client.py` → `get()` and `paginate()`: retry decisions, waits, rate-limit handling.
4. `warehouse.py` → `load_silver()`: validation, dead letter, in-batch de-duplication, guarded upsert.
5. `quality.py`: each check, and `enforce()`.
6. `sql/gold.sql`: the business-facing tables.

Then read the tests in `api-ingestion-learn/tests/`. Each test name is a sentence describing a production incident.

```bash
cd api-ingestion-learn && python -m pytest -v        # 20 tests, about 20 seconds
```

## Where this differs from a production setup (know these for interviews)

| Here | In a company |
|---|---|
| A script you run | An orchestrator (Airflow/Dagster/Prefect) schedules it, retries tasks, alerts on failure (see `api-ingestion-learn/dags/`) |
| DuckDB file + local Parquet | A cloud warehouse (Snowflake/BigQuery/Databricks) + object storage (S3/GCS) |
| Row-by-row Python validation | Validation in SQL/Spark, dbt tests, or a quality tool |
| One endpoint, one repo | Many endpoints with dependencies (fetch list, then each item), concurrency sharing a rate-limit budget |
| Hand-written client | Often a managed connector (Fivetran, Airbyte) for common sources; custom code for the long tail |
| Secrets from env | A secrets manager |
| Print + a run table | Metrics, dashboards and alerts (freshness SLAs, failure alerts) |

## Try it yourself (each is a realistic ticket)

1. **Backfill:** reset the watermark to 30 days ago and re-run. What does the pipeline do, and why is it safe?
2. **New column:** add `milestone` to the silver table using `make replay`-style rebuild, no API calls.
3. **Alert rule:** write a SQL query over `ops.pipeline_runs` that finds "no successful run in the last 2 hours".
4. **Break it on purpose:** change `publish()` to move the watermark *before* silver loads (the lesson 6 bug) and find which test notices.

## Check yourself (explain without looking)

1. Walk through what happens if the process is killed (a) during extract, (b) after silver load but before publish. What state is left, and how does the next run recover?
2. Which stage would you move to a separate Airflow task first, and why?
3. A stakeholder says "yesterday's numbers changed". Using the run log, bronze and dead letters, how do you investigate?

<details><summary>Answers</summary>

1. (a) Some bronze files may exist, silver and watermark are untouched; next run re-extracts the same window. (b) Silver has the new rows, the watermark hasn't moved; next run re-reads and re-upserts the same rows (idempotent), then moves the watermark. No manual cleanup in either case.
2. Typically extract (the part that depends on the external API and needs its own retry policy and timeouts), so a transform failure doesn't force re-fetching.
3. Look at `ops.pipeline_runs` for runs that touched yesterday (statuses, row counts, retries), check `ops.dq_results` and `silver.dead_letter` for rejects or failed checks, and compare with bronze payloads for the affected ids. Late-arriving updates (lookback window) are a common cause.
</details>

**Next:** [Lesson 11: Webhooks](../11_webhooks/README.md)
