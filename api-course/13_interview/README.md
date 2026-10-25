# Lesson 13: Interview practice

**Time:** 90 min, then repeat weekly · **Needs:** Lessons 1-10 (11-12 optional)

By now you've watched each failure happen and fixed it. This lesson turns that into answers. In interviews, API ingestion rarely appears as its own round. It shows up inside
**pipeline design questions** and **"what would you do if..." follow-ups**. Prepare for both.

## How to practise

1. Cover the answer, say yours **out loud** (typing is not the same skill), then compare.
2. Aim for 30-60 seconds: *principle first, then the failure it prevents, then how you'd implement it.*
3. Where possible, add "I tested this by..." from the lessons. Experience beats recitation.

## Part A: Concept questions

**Q1. How do you make an API ingestion job safe to re-run?**
Idempotent writes: a stable key per record and an upsert (`MERGE`/`ON CONFLICT`) with an `updated_at` guard so stale data can't overwrite newer. Move the watermark only after a successful, validated load. Result: at-least-once delivery plus an idempotent sink behaves like exactly-once. *(Lessons 6, 7)*

**Q2. How do you load incrementally from an API?**
Persist a watermark (max `updated_at` loaded) per source, request `since=watermark` (inclusive), and re-read a small overlap, relying on idempotency. Take the watermark from the data, not the wall clock. Add a periodic full reconciliation for deletes and late updates. *(Lesson 6)*

**Q3. An API returns 500s intermittently. What do you do?**
Retry only transient errors (5xx, 429, timeouts) with capped exponential backoff and jitter, honour `Retry-After`, limit attempts, and log each retry. Don't retry 4xx. The orchestrator retries the task for longer outages. Retried calls must be idempotent. *(Lesson 4)*

**Q4. What is jitter and why use it?**
Randomness added to backoff so clients that failed together don't retry together and re-overload the service (a retry storm). *(Lesson 4)*

**Q5. How do you handle rate limits?**
Read the limit headers, slow down before reaching zero, wait until reset (or fail fast if the wait is long so the scheduler can reschedule), share one budget across workers, and reduce calls via incremental loads, conditional requests and bulk endpoints. *(Lesson 5)*

**Q6. Offset vs cursor pagination?**
Offset: simple, but slow on deep pages and unreliable when data changes (shifts cause duplicates or skips). Cursor/keyset: continues after a key, stable under change, fast. Consuming: follow `next` links, don't compute URLs. Prefer an order where change causes duplicates, not gaps. *(Lessons 3, 12)*

**Q7. How do you handle schema changes from an API?**
Land raw data first. Validate against an explicit contract; ignore new fields, reject records with missing/invalid required fields into a dead-letter table, alert on reject rate, and replay from raw once the contract is updated. *(Lesson 9)*

**Q8. What's the medallion architecture?**
Bronze = raw as received; silver = cleaned, typed, de-duplicated; gold = business aggregates. Everything downstream derives from bronze so it can be rebuilt. *(Lesson 9)*

**Q9. What is an ETag?**
A response fingerprint; the client sends it back in `If-None-Match` and gets `304` with no body if unchanged. Saves bandwidth and often quota. Beware: it covers one response (page 1), so ordering must put changes there. *(Lesson 8)*

**Q10. Webhooks vs polling?**
Webhooks: low latency, no wasted calls, but you need a public endpoint, signature verification, dedupe, and tolerance for lost/out-of-order events. Polling: simple and replayable but laggy and quota-hungry. Many systems do both. *(Lesson 11)*

**Q11. How do you secure a webhook endpoint?**
Verify an HMAC signature over the raw body with a constant-time compare; fail closed if no secret; ack fast; optionally reject old timestamps (replay protection). *(Lesson 11)*

**Q12. 401 vs 403?**
401: not authenticated (who are you?). 403: authenticated but not permitted. Neither is fixed by retrying. *(Lesson 2)*

## Part B: Scenario questions ("what would you do if...")

Practise structuring: **clarify → hypothesise → check → fix → prevent.**

1. **"Yesterday's dashboard numbers doubled."** Likely duplicates from a non-idempotent re-run or overlapping window. Check run history for retried/duplicate runs, count rows per natural key, look for appends without upsert. Fix with key-based upsert/de-dupe; prevent with an idempotency test and a uniqueness check in the quality gate.
2. **"The job succeeded but data is missing."** Pagination or default filters (`state=all`?), watermark advanced before load, rows rejected silently, page-shift gaps. Reconcile counts against the source; inspect dead letters; check the sort order; compare bronze with silver.
3. **"We keep getting 429s during a backfill."** Compute the request budget, throttle proactively, honour `Retry-After`, reduce request count (bigger pages, bulk endpoints), spread the backfill over time, share the budget across workers.
4. **"The pipeline works for a month, then fails with 401."** Expired or rotated token, a disabled owner account, OAuth refresh not implemented. Add refresh logic or fail fast with a clear alert; monitor token expiry.
5. **"The provider added a field and our job failed."** Contract too strict (`extra=forbid`) or schema-on-write failure. Ignore unknown fields, keep raw, alert on *removals* of fields you need.
6. **"Webhooks stopped arriving for 3 hours while we were down."** Providers may retry for a limited time; run a reconciliation poll for the gap using the watermark; the idempotent handler absorbs overlaps.
7. **"We need to backfill two years."** Chunk by time, checkpoint progress, respect rate limits, run idempotently so chunks can be retried, land raw first, and validate counts per chunk.

## Part C: The design question (use this skeleton)

> *"Design a pipeline that ingests data from a vendor's REST API into our warehouse, every 15 minutes."*

Talk through, in this order:

1. **Clarify:** volume, latency need, API limits, auth type, does it have `updated_at` filtering / webhooks / bulk export, how are deletes represented, who consumes the output.
2. **Extract:** auth + secrets; pagination; incremental with watermark; retries/backoff/rate-limit; ETag if supported.
3. **Land raw:** object storage, partitioned by ingest date/run id, immutable.
4. **Transform & load:** validate against a contract, dead-letter bad rows, idempotent `MERGE` into silver by natural key with a freshness guard.
5. **Quality gate:** reject rate, uniqueness, nulls, volume anomalies, freshness; block publishing and the watermark on failure.
6. **Orchestration:** tasks per stage, task-level retries, `max_active_runs=1` (no overlapping runs on one watermark), SLAs.
7. **Observability:** run table (status, volumes, retries), freshness metric, alerts on failures/stale data/dead-letter growth.
8. **Evolution:** replay from raw, backfills, schema drift handling, deletes via reconciliation, cost.
9. **Trade-offs:** managed connector (Fivetran/Airbyte) vs custom; webhook + poll hybrid; where it would break at 100x scale.

If you can walk this in 10 minutes with reasons for each step, you're in good shape.

## Part D: Talking about the project

**30-second version:**
> "I built a course and two reference projects around API ingestion. The pipeline pulls GitHub issues incrementally with retries and rate-limit handling, lands raw data, validates it into a deduplicated silver table with a data-quality gate that blocks bad loads, and supports replay. I wrote tests for each failure mode, like pagination skipping rows and a crash after the watermark moves. A second project serves the data through a cursor-paginated API and receives signed webhooks with an inbox for dedupe and ordering."

**Be ready to be honest about limits:** it's a single machine, DuckDB not a cloud warehouse, a mock server, no real orchestrator running. That's fine, and saying so confidently reads as maturity.
Then say how you'd productionise it: Airflow tasks per stage, S3 + a warehouse, `MERGE`, dbt tests, secrets manager, alerts.

**Details interviewers like:** "I chose newest-first pagination because shifting rows then cause duplicates rather than gaps, and I demonstrated the difference with a test" or "the watermark moves last, and I tested the crash case."

## A candid note on tools

Interviews and job posts mention **tools**: Airflow, dbt, Snowflake/BigQuery/Databricks, Spark, Docker, Git, cloud basics. Recruiters and filters match on those keywords.
This course teaches the *concepts* that interviewers probe once you're talking to them. Do both: check the postings for the roles you want, learn the 2-3 tools they repeat,
and use the pipeline here as the project to run on them (e.g. orchestrate it with Airflow, build the gold layer in dbt). The concepts transfer to any tool.

## Self-test checklist (can you do these without looking?)

- [ ] Explain idempotency and give three places the pipeline relies on it
- [ ] Draw the six-stage pipeline and say what happens if it crashes at each stage
- [ ] Explain why oldest-first pagination can lose data
- [ ] Explain why the watermark moves last
- [ ] List what to retry and what not to, and explain jitter
- [ ] Explain how you would detect and handle a provider's schema change
- [ ] Walk the design-question skeleton in 10 minutes
- [ ] Describe one thing in the project that you'd change at 100x scale
