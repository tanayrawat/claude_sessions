# Lesson 9: Raw data, contracts and bad records

**Time:** 45 min · **Needs:** Lessons 3, 7 · **Run:** `python api-course/09_raw_and_contracts/demo.py`

## The problem

An API is a contract you don't control. Over months it will: add fields, send `null` where it never did, send a status value nobody documented,
change a number into a string, return a record with a missing `id`. If your pipeline **crashes** on each oddity, you're paged weekly. If it **silently
drops or silently accepts** them, your numbers are wrong and nobody knows. And later someone will ask for a field you ignored, and the API can only give you
*today's* data, not what it said last March.

## The idea: two defences

### Defence 1: validate against an explicit contract, and quarantine failures

Write down what you accept (here, a pydantic model: `ghpipe/models.py`). Every incoming record is checked:

| Situation | Policy | Why |
|---|---|---|
| **Unknown extra fields** (provider added `reactions`) | **Ignore, don't fail** | Additive changes shouldn't break you; the data is still in the raw copy |
| **Required field missing/null** (`title: null`) | **Reject to a dead-letter table** with the reason | Silent acceptance corrupts the clean layer |
| **Value outside the allowed set** (`state: "merged"`) | **Reject to dead-letter** | You don't know what it means; guessing is dangerous |
| **Optional gaps** (`user: null`, a deleted account) | **Model them as optional** | Real data has holes; it's not an error |
| **Records that aren't what you want** (PRs inside the issues list) | **Flag** and decide downstream | Keep the information, filter later |

The **dead-letter table** holds the rejected payload + the error message + the run id. Someone investigates; once the contract or the source is fixed,
you reprocess them. Rows are never silently dropped, and one bad row never blocks 9,999 good ones.

### Defence 2: keep the raw data ("bronze")

Before parsing anything, save the response exactly as received (here: Parquet files partitioned by ingest date and run id, written atomically).

```
v1 parser output : {'id': 2, 'title': 'Login broken'}
v2 parser output : {'id': 2, 'title': 'Login broken', 'reactions': 3}   <- from SAVED data, zero API calls
```

When the parser had a bug, or someone wants a new column, you **re-run your code on the stored raw data** (a *replay*) instead of re-extracting from an API
that may return different data, rate-limit you, or no longer have the old state.

### The layers (medallion architecture)

| Layer | Contains | Who uses it |
|---|---|---|
| **Bronze** (raw) | Exactly what the source sent, append-only, plus metadata (when, which run, which URL) | Engineers, for replay and debugging |
| **Silver** (clean) | Validated, typed, de-duplicated, one row per entity | Analysts, data scientists, other pipelines |
| **Gold** (marts) | Aggregates shaped for a question ("issues closed per day") | Dashboards, reports |

You'll hear these names constantly (Databricks popularised them), but the idea is old: *never destroy your source of truth; derive everything else from it.*

## Quality gates (a quick preview of lesson 10)

After loading silver, run checks *before* publishing: reject rate under a threshold, ids unique, timestamps sensible (`updated_at >= created_at`),
row count didn't shrink, data is fresh. A failed check **blocks** the run so bad data never reaches gold, and the watermark does not move, so the next run retries the same window.
Tools you'll meet: dbt tests, Great Expectations, Soda, Monte Carlo.

## Run it

**Predict:** of the six hand-made records (clean, extra fields, null title, bad state, null user, a PR), which are accepted?

```
  accepted ids: [1, 2, 5, 6]
  REJECTED id=3 -> title: Input should be a valid string
  REJECTED id=4 -> state: Input should be 'open' or 'closed'
  id=2 had unknown fields (reactions, type): accepted anyway   -> drift must not break the pipeline
  id=5 had user=None: accepted (user=None)                     -> real data has holes; model them
  id=6 is a pull request: is_pull_request = True               -> flag it, decide downstream
```

## In real jobs

- Data contracts are a hot topic: producers and consumers agree on schemas and get alerted on breaking changes. For third-party APIs you can't impose one, so you defend yourself like this.
- At scale you validate in SQL/Spark rather than row-by-row Python, but the policy table above is the same.
- Alert on the *rate* of rejects, not just their existence: a jump from 0.1% to 12% means the source changed.
- Storage format: Parquet (columnar, compressed, typed) for bronze/silver on object storage; JSON/JSONL is fine for tiny volumes; warehouses can store raw JSON in `VARIANT`/`JSON` columns.

## Where it is in the repo

`ghpipe/models.py` (contract), `ghpipe/bronze.py` (raw landing), `ghpipe/warehouse.py` → `load_silver()` and `rebuild_silver()`,
`ghpipe/quality.py`; tests `test_bad_records_are_quarantined_and_drift_is_tolerated`, `test_replay_rebuilds_silver_from_bronze_without_the_api`.

## Try it yourself

1. Add a record with `"id": "abc"` (a string). Does it pass? Look up pydantic's strict vs lax mode.
2. Add a new required field `priority: str` to the model. What happens to records lacking it, and what does your reject rate look like?
3. Write a v3 parser that extracts `type.name` into a column, again from stored data only.

## Check yourself

1. A provider adds a new field. Should your pipeline fail? What if it removes a field you use?
2. Why store raw data if you already store the cleaned version?
3. What belongs in a dead-letter record besides the bad payload?
4. A run rejects 40% of its records. What should happen automatically?

<details><summary>Answers</summary>

1. Adding: no, ignore it (it's preserved in the raw copy). Removing a *required* field is a breaking change: records get rejected, the reject-rate check should fail the run and alert.
2. To reprocess without calling the API: fix parser bugs, add columns, rebuild after a logic change; also for audit and debugging.
3. The error reason, source identifier, run id, and timestamp, so someone can diagnose and reprocess.
4. The data-quality gate should block publishing and the watermark, and alert; a 40% reject rate means the source or your contract is broken.
</details>

**Next:** [Lesson 10: The whole pipeline](../10_capstone_pipeline/README.md)
