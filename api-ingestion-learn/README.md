# API Ingestion for Data Engineers — a production-shaped pipeline

> **New to this?** Start with the [API course](../api-course/README.md). It teaches each idea in this project one lesson at a time, with demos that show every failure first. This README is the reference for the finished project (it's the capstone for lessons 3-10).

Most data engineering work is not "write a Spark job"; it is **getting data out of someone else's API
reliably, repeatedly and cheaply, and landing it somewhere analysts can trust**. This project does exactly that
for the GitHub Issues REST API, with every concern a real ingestion job has — and a tiny test suite that proves each one.

```
  GitHub REST API  (or the local mock: same quirks, fault injection, no network needed)
        │  auth · pagination (Link header) · retries+jitter · rate limits · ETag/304 · incremental `since`
        ▼
  ┌─ extract ─────────────┐   ┌─ BRONZE ──────────────────────┐   ┌─ SILVER (DuckDB) ─────────────┐   ┌─ GOLD ──────────────┐
  │ ApiClient.paginate()  │──▶│ raw JSON, as received          │──▶│ pydantic contract → typed rows │──▶│ daily_issue_activity │
  │ watermark + etag state│   │ Parquet, ingest_date/run_id    │   │ idempotent upsert (id, newest) │   │ label_resolution     │
  └───────────────────────┘   │ immutable → enables REPLAY     │   │ bad rows → silver.dead_letter  │   │ open_backlog         │
                              └────────────────────────────────┘   └───────────────┬───────────────┘   └──────────▲──────────┘
                                                                          DQ gate ──┘  fail → watermark NOT advanced │
                                                                          ops.pipeline_runs / ops.dq_results ────────┘
```

DuckDB + local Parquet stand in for Snowflake/BigQuery + S3. The patterns are identical; only the I/O changes.

## Run it (≈2 minutes)

```bash
cd api-ingestion-learn
pip install -r requirements.txt
make test                         # 20 tests, ~20s, no network

# terminal 1: a hostile fake GitHub (15% random 429/500/502, 40 req / 10s quota)
make mock-flaky
# terminal 2:
make run                          # full load: 250 rows, 3 pages. Watch for retry/throttle log lines
make run                          # incremental: only rows with updated_at >= watermark
make run                          # status=no_change, 1 request, HTTP 304
make mutate                       # source changes: 8 edits (3 closed), 4 new issues
make run                          # inserted=4 updated=8 — nothing else re-pulled
make mutate-bad && make run       # 2 malformed rows → DQ gate FAILS the run, watermark stays put
MAX_REJECT_RATE=0.5 make run      # operator accepts it → succeeds, bad rows sit in silver.dead_letter
make runs                         # the run log you would look at when a dashboard is stale
make sql                          # query gold + DQ results
make replay                       # rebuild silver+gold purely from bronze, zero API calls
```

**Point it at real GitHub:** `API_BASE_URL=https://api.github.com GITHUB_TOKEN=<pat> GITHUB_REPO=apache/airflow python -m ghpipe run`
(not `make run` — the Makefile forces the fake mock token on purpose, so a real token is never sent to localhost)
(unauthenticated = 60 req/h, token = 5,000 req/h; start with a small repo).

## Where each real-world concern lives (and the test that proves it)

| Concern | What goes wrong in production | Code | Test |
|---|---|---|---|
| **Auth** | Hardcoded/leaked tokens; 401 retried forever | `Settings.from_env`, `ApiClient` (bearer), 401 → `ApiError` no retry | `test_does_not_retry_client_errors` |
| **Pagination** | Stopping at page 1; building page URLs by hand | `ApiClient.paginate` follows `Link: rel="next"` | `test_pagination_follows_link_header` |
| **Silent incompleteness** | API default is `state=open` → every closed issue missing, no error | `pipeline.extract` sets `state=all` | `test_state_defaults_to_open_only` |
| **Transient failures** | One 502 at 3am kills the DAG | retry 5xx/429/network, exponential backoff **with full jitter** | `test_retries_5xx_then_succeeds`, `test_backoff_is_jittered_and_capped` |
| **Rate limits** | Hammering past quota → 403/ban; or sleeping 1h inside a task | honour `Retry-After`/`X-RateLimit-Reset`, throttle at 1 remaining, **raise** `RateLimitExceeded` if wait > `max_wait` | `test_honours_retry_after_on_429`, `test_proactively_waits_for_quota_reset`, `test_refuses_to_sleep_for_an_hour` |
| **Incremental load** | Full reload every run = slow, expensive, quota-burning | watermark = max `updated_at`, sent as `since` | `test_incremental_run_only_pulls_changes` |
| **Conditional requests** | Polling an unchanged source burns quota | `ETag` / `If-None-Match` → 304 (free on GitHub) | `test_polling_with_no_changes_becomes_a_304` |
| **ETag trap** | Page-1 ETag unchanged while new data lands on page N → silent miss | `direction=desc` puts new data on page 1 | `test_etag_does_not_hide_changes_on_later_pages` |
| **Idempotency** | Re-run / retry / backfill creates duplicates | `INSERT … ON CONFLICT (id) DO UPDATE … WHERE excluded.updated_at >= existing` | `test_pipeline_is_idempotent` |
| **Out-of-order data** | Replaying old data overwrites newer | the `updated_at` guard above | `test_stale_data_never_overwrites_newer` |
| **Schema contract** | Provider nulls a field → whole load dies or bad data flows silently | pydantic `Issue`; failures → `silver.dead_letter` | `test_bad_records_are_quarantined_and_drift_is_tolerated` |
| **Schema drift** | Provider adds a field → pipeline breaks | `extra="ignore"` in silver; raw kept in bronze | same test |
| **Data-quality gate** | Bad batch published, then discovered by a stakeholder | `quality.py`; error-severity failure blocks the watermark | `test_dq_failure_blocks_watermark_and_recovers` |
| **Replay / backfill** | Logic bug found a month later; API no longer returns old state | bronze is immutable; `ghpipe replay` | `test_replay_rebuilds_silver_from_bronze_without_the_api` |
| **Observability** | "Is the data fresh? What happened last night?" | `ops.pipeline_runs`, `ops.dq_results`, retry/request counts per run | `make runs` |
| **Orchestration** | Overlapping runs racing on one watermark | stage functions → Airflow tasks, `max_active_runs=1` | `dags/github_issues_dag.py` (illustrative, not executed here) |

## Reading order (each file is short and does one thing)

1. `ghpipe/mock_server.py` — skim the docstring. Knowing how the API *misbehaves* is half of API ingestion.
2. `ghpipe/client.py` — the retry/rate-limit policy. Then `tests/test_client.py`.
3. `ghpipe/pipeline.py` — `extract` (params, watermark, ETag) and `run_pipeline` (stage order; watermark moves last).
4. `ghpipe/bronze.py` → `ghpipe/warehouse.py` (`load_silver`) → `ghpipe/quality.py` → `sql/gold.sql`.
5. `tests/test_pipeline.py` — each test is a production incident in miniature.

## Decisions you should be able to defend

- **Why land raw bronze before parsing?** The API is not a time machine. If silver logic is wrong, or someone wants a field we ignored, we reprocess from bronze. Re-calling the API costs quota and returns *current* state, not history.
- **Why move the watermark last?** Anything that fails earlier (API, parse, DQ) leaves the window unchanged, so the next run re-reads it. That only works because the load is idempotent. *At-least-once delivery + idempotent sink = effectively exactly-once.*
- **Why `since` is inclusive and `>=` in the upsert?** Providers stamp `updated_at` at 1-second resolution. `>` would skip a second edit in the same second; re-reading one boundary row is free. Use `LOOKBACK_SECONDS` to also re-read a window behind the watermark if the provider has eventual consistency.
- **Why `direction=desc`?** Offset pagination over a live list shifts rows. Ascending-by-updated can *skip* rows (an item you already read moves to the end, pulling the rest up). Descending can only *duplicate*. Prefer duplicates to gaps.
- **Why does the first 304 take three runs?** An ETag only matches the exact request. The full load has no `since`; the first incremental run adds one, so it can't reuse the ETag; the third run can.
- **Why raise on a long rate-limit wait instead of sleeping?** A worker slot held for an hour is expensive and hides the problem. Failing fast lets the orchestrator reschedule (`retry_delay`) and alert on repeated failures.
- **Why a DQ *gate* and not just dashboards?** Once bad data is published, trust is gone. Blocking the watermark makes the failure visible and cheap to fix.

## How this maps to what companies actually run

| Here | In industry |
|---|---|
| `ApiClient` + `extract` | Custom Python/Spark connectors, or managed connectors (Fivetran, Airbyte, Stitch) — **you still need to know this** to build the long tail they don't cover, debug them, and evaluate build-vs-buy |
| bronze/silver/gold | Medallion layout on S3/ADLS/GCS + Delta/Iceberg/Hudi; raw → staging → marts |
| `load_silver` upsert | `MERGE INTO` on Snowflake/BigQuery/Delta; dedupe with `ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC)` at scale |
| `sql/gold.sql` | dbt models (+ dbt tests ≈ `quality.py`); Great Expectations/Soda/Monte Carlo for DQ |
| `dags/` | Airflow / Dagster / Prefect; secrets via Connections or Vault/Secrets Manager |
| `ops.pipeline_runs` | Run metadata → Datadog/Grafana alerts, SLAs on freshness |
| polling + `since` | Prefer **webhooks/CDC/streams** when offered (events → Kafka → this same bronze/silver). See `../kafka-learn` for that side |

## Honest limits (say these in the interview before they ask)

- Single-process, single-writer DuckDB; pydantic validation is row-by-row Python. At millions of rows you'd validate in SQL/Spark and write to a warehouse with concurrent writers.
- One endpoint. Real sources add sub-resources (comments, events) → fan-out requests, parent/child watermarks, and concurrency with a shared rate-limit budget (token bucket).
- No deletes: GitHub issues are never hard-deleted, but many APIs are. You'd need a periodic full reconciliation or a tombstone feed.
- The mock server mimics GitHub's shape, not every behaviour (e.g. secondary rate limits, GraphQL cost-based limits).

## Exercises (each is a realistic ticket)

1. **Add `comments` as a child resource.** Per-issue calls multiply requests → add bounded concurrency (`ThreadPoolExecutor`) sharing one rate-limit budget. Where does the child watermark live?
2. **Hard deletes:** add `DELETE` support to the mock; detect missing ids with a weekly full-reconciliation job and soft-delete in silver (`_is_deleted`).
3. **Schema evolution:** start using `reactions` from the drifted record — add the column and backfill via `make replay`, no API calls.
4. **SCD2 for issue state** (open → closed history) instead of overwriting; compute reopen rates.
5. **Swap the sink** to Postgres or BigQuery; keep bronze on disk. Which tests still pass unchanged?
6. **Alerting:** emit run metrics to Prometheus/StatsD; alert if `now - max(updated_at)` > SLA or `http_retries` spikes.
7. **Run the DAG** in a local Airflow (`astro dev init`) and make the 429 scenario drive an Airflow-level retry.
