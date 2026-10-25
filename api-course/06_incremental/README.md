# Lesson 6: Incremental loading

**Time:** 60 min · **Needs:** Lessons 3-5 · **Run:** `python api-course/06_incremental/demo.py`

## The problem

Your first run loads all 250 issues. Fine. The job runs every 15 minutes. If each run reloads everything, you re-download 250 rows to learn that 12 changed.
At 250 rows it's wasteful; at 50 million it's impossible: it blows the rate limit, the runtime, and the bill. You need to ask the source
**"what changed since I last looked?"** and carry only that.

## The idea: the watermark

Keep a **bookmark** (the *watermark*): the largest `updated_at` you have successfully loaded. Each run:

1. Read the watermark (empty on the first run → full load).
2. Ask the API for records changed since then (`?since=<watermark>`), or filter by `updated_at` yourself if the API can't.
3. Load them.
4. Only after success, move the watermark forward to the largest `updated_at` you saw.

```
run 1:  since=(none)            -> 250 rows   -> watermark = 21:43:31
        ... source changes: 8 edits, 4 new ...
run 2:  since=21:43:31          -> 13 rows    -> watermark = (the newest of those)
```

Requirements for this to work: the source must have a **reliable "last changed" field** (`updated_at`, `modified`, a sequence id) and an API
that can filter on it. If it has neither, you'll need other approaches (full snapshots and diffs, change feeds, webhooks: lesson 11, or database CDC).

### Details that decide whether it's correct

| Detail | Why it matters |
|---|---|
| **Inclusive boundary** (`>=`) | Timestamps have limited precision (often whole seconds). Two changes can share a timestamp, and a strict `>` could skip one. Re-reading the boundary row is cheap *if loads are idempotent* (lesson 7). The demo shows 13 rows = 12 changes + 1 boundary row |
| **Lookback window** | Some systems commit late (a row stamped 10:00 appears at 10:03). Subtract a few minutes from the watermark each run and rely on idempotency to absorb the overlap |
| **Use the source's clock** | Take the watermark from the *data* (max `updated_at` seen), not from your server's clock (`now()`). Clock skew between systems silently loses or duplicates data |
| **Moves forward only** | Never let the watermark go backwards after an empty or replayed run |
| **Deletes are invisible** | A deleted record doesn't show up as "changed". Needs a deletion feed/tombstones or a periodic full reconciliation |
| **Per-source state** | Each source/endpoint/customer has its own watermark, stored durably (a table), not in memory |

### The order of operations is the whole game

The demo's part 4 simulates a crash while loading, under two different orderings:

```
bookmark moved BEFORE loading :  8 changed rows permanently lost
bookmark moved AFTER loading  :  0 changed rows lost
```

If you advance the bookmark first and the job dies, the next run asks for "changes since the new bookmark", which are *after* the rows you never stored. They're gone, and nothing
reports an error. If you advance it **last**, a crash just means the next run re-reads the same window. Combined with an idempotent load that is harmless.

> **At-least-once delivery + idempotent sink = effectively exactly-once.** This sentence is a staple of data engineering interviews.

### Full refresh is sometimes right

Small reference tables (a list of 200 countries, a few hundred products) are simpler and more robust to reload in full every run. Use incremental loading when
volume or rate limits demand it, and add a periodic full reconciliation to catch what incremental logic can't see (deletes, late updates).

## Run it

**Predict:** after the source gets 8 edits and 4 new issues, how many rows does the incremental run fetch, and how many requests does it take vs the full reload?

```
  fetched 250 rows in 3 requests;  watermark = 2026-10-25T21:43:31Z
--- 3a. Naive second run: reload everything
  254 rows, 3 requests
--- 3b. Incremental second run: ?since=<watermark>
  13 rows, 1 request
--- 4. WHEN you move the bookmark matters
  bookmark moved BEFORE loading :  8 changed rows permanently lost
  bookmark moved AFTER loading  :  0 changed rows lost
```

(Why 8, not 12, lost in the first case? The 4 new issues share the newest timestamp, and since `since` is inclusive they get re-fetched by luck. The 8 edited ones are older than the new watermark and vanish.)

## In real jobs

- Watermark state usually lives in a small control table in the warehouse (as here: `ops.pipeline_state`) or in the orchestrator's variables.
- Airflow's built-in alternative is **data interval** partitioning: each run processes `[data_interval_start, data_interval_end)`. It makes backfills trivial (re-run a past interval) and is common for APIs that filter by date range.
- A **backfill** (reloading history after a bug or a new column) is just "run with an older watermark". Your pipeline must support it deliberately.

## Where it is in the repo

`ghpipe/pipeline.py` → `extract()` (computes `since`), `publish()` (moves the watermark last, never backwards);
`ghpipe/warehouse.py` → `get_state` / `set_state`; tests `test_incremental_run_only_pulls_changes`, `test_dq_failure_blocks_watermark_and_recovers`.

## Try it yourself

1. In part 4, make `since` exclusive by adding 1 second to the watermark. Can you construct a case where a change is lost? (Two edits within the same second.)
2. Add a lookback of 60 seconds. How many rows does the incremental run now fetch, and why is that OK?
3. What would you do about deleted issues? Sketch a solution.

## Check yourself

1. Why take the watermark from the data and not from `datetime.now()`?
2. Your job crashes after loading but before saving the watermark. What happens next run, and is that a problem?
3. The API has no `updated_at` filter. What are your options?
4. Why is the "advance the watermark last" rule useless without idempotency?

<details><summary>Answers</summary>

1. Your clock and the source's clock differ, and records can be committed slightly late. A time from the data itself is consistent with the source's own ordering.
2. The run re-reads the same window and loads the rows again. Not a problem if the load is idempotent (upsert); a disaster if it's a plain insert.
3. Pull everything and diff against your copy (fine for small data), use a different change feed/event stream/webhooks, partition by a date field the API *can* filter on, or ask the provider for an export.
4. Re-reading the window after a failure produces duplicates unless repeated loads have no additional effect.
</details>

**Next:** [Lesson 7: Idempotency](../07_idempotency/README.md)
