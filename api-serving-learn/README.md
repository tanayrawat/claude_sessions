# Serving Data Over APIs + Receiving Webhooks (Part 2)

[`../api-ingestion-learn`](../api-ingestion-learn) taught you to be an API **client**: pull data out of someone else's API.
This project flips it. You become the **server**:

1. **Serve** warehouse data to other teams/apps through a safe, paginated, rate-limited API.
2. **Receive** webhooks (push instead of poll) without losing, duplicating or mis-ordering events.

Doing both sides is the point: when you have written the thing that *returns* `429 Retry-After`, you understand
why part 1's client behaves the way it does.

```
 part 1 pipeline ──▶ warehouse (silver.issues, gold.*) ──▶  GET /v1/issues  ──▶ dashboards, apps, other teams
                                                            (auth · rate limit · cursor paging · ETag)

 GitHub ──signed POST──▶ /webhooks/github ──▶ inbox (SQLite) ──▶ worker ──▶ issue_state
                         verify · dedupe · ack 202 fast          retry · dead-letter · order-safe
```

## How to learn from this (read this first)

Do **not** read all the code and then run it. Work in this loop, per stage:

1. **Read the file's docstring only.** It states the problem and the rule.
2. **Predict** what the "Try it" step will print. Write it down.
3. **Run it**, compare with your prediction, and explain any difference.
4. **Break it** (the "Break it" step), then run the test that guards against that bug and watch it fail.
5. **Explain it out loud** in two sentences, as if to an interviewer. If you can't, re-read.

Setup (once):

```bash
cd api-serving-learn
pip install -r requirements.txt
make test            # 17 tests, ~8s. Everything below is already proven here.
make seed            # demo warehouse with 500 issues (many tied timestamps on purpose)
make serve           # terminal 1. API docs: http://127.0.0.1:8000/docs  (try requests in the browser)
```

Demo API keys (from the Makefile): `demo-key`, `bi-key`. Webhook secret: `s3cret`. Terminal 2 for everything below.

---

## Stage 1 — Read API: pagination that doesn't lie

**Concept:** return big result sets in pages without duplicates, gaps or slow deep pages.
**Read:** `app/pagination.py` (docstring), then `list_issues` in `app/main.py`.

**Try it**
```bash
H='X-API-Key: demo-key'
curl -s -H "$H" "localhost:8000/v1/issues?limit=2&state=open"      # note next_cursor
curl -s -H "$H" "localhost:8000/v1/issues?limit=2&state=open&cursor=<paste it>"
```
*Predict:* will page 2 start exactly where page 1 ended? What is inside the cursor?
(`echo <cursor> | base64 -d` — it's just the last row's `(updated_at, id)`.)

**Why it matters in real jobs:** every internal data API and every SaaS API you will ingest uses cursors or
offsets; knowing the failure modes lets you design and debug both.

**Break it:** in `list_issues` change `(updated_at, id) < (...)` to `updated_at < ...` (drop the `id` tie-breaker) and
run `pytest -k tied_timestamps`. The seed has many rows with identical timestamps, so rows vanish at page boundaries.
Also run `pytest -k keyset_is_stable`: this test inserts a new row between page 1 and 2 — the exact situation where
`OFFSET` pagination returns a duplicate.

**Also notice:** `limit` is capped at 200 (`Query(le=200)`), filters are bound `?` parameters (try the SQL-injection
strings in `test_bad_inputs_are_clean_4xx_not_500`), `/health` vs `/ready` are separate on purpose (docstrings explain).

## Stage 2 — Protecting the API: auth, rate limits, ETags

**Concept:** you can't let one consumer's runaway loop take down the warehouse for everyone.
**Read:** `app/security.py` (all of it; it's short).

**Try it**
```bash
curl -s -o /dev/null -w "%{http_code}\n" localhost:8000/v1/issues                 # ?
for i in $(seq 1 35); do curl -s -o /dev/null -w "%{http_code} " -H 'X-API-Key: demo-key' localhost:8000/v1/issues; done; echo
curl -si -H 'X-API-Key: bi-key' localhost:8000/v1/issues | grep -i ratelimit       # a different client: its own bucket
ET=$(curl -si -H 'X-API-Key: demo-key' localhost:8000/v1/issues | grep -i '^etag' | cut -d' ' -f2 | tr -d '\r')
curl -si -H 'X-API-Key: demo-key' -H "If-None-Match: $ET" localhost:8000/v1/issues | head -1   # ?
```
*Predict:* at which request does `429` start (bucket = 30, refill 1/s)? What does the `bi-key` client see?
What status do you get with the ETag, and how many bytes in the body?

**Connect to part 1:** open `../api-ingestion-learn/ghpipe/client.py` and find where it reads `Retry-After`,
`X-RateLimit-Remaining` and sends `If-None-Match`. This server is the other end of that conversation.

**Break it:** in `make_guard` return the same client name for every key, then run `pytest -k per_client`.

**Real-world mapping:** at most companies this layer is an **API gateway** (Kong, Apigee, AWS API Gateway, Envoy)
with Redis-backed limits and an identity provider (OAuth2/JWT, not static keys). You still need to know the algorithm
and the headers. Key hashing is shown in `settings.hash_key`.

## Stage 3 — Webhooks: reliable push ingestion

**Concept:** instead of polling GitHub, GitHub calls *you*. Delivery is **at-least-once and unordered**, and you must
answer fast. That forces the "inbox" design. **Read:** the docstring of `app/inbox.py`, then `github_webhook` in `app/main.py`.

**Try it** (server from `make serve` must be running)
```bash
make webhooks     # 8 deliveries, including a retry, a stale event, a forgery, and a poison message
make worker       # processes the inbox
curl -s -H 'X-API-Key: demo-key' localhost:8000/v1/admin/inbox
sqlite3 data/inbox.sqlite 'select id,state,title,updated_at from issue_state'   # or use python -c with sqlite3
```
*Predict before running:* what HTTP code does each of the 8 lines get? After `make worker`, what are the
final `state` and `title` of issue 1000900, given delivery 5 (a stale edit) arrived last? How many messages end
up `dead`, and why is "unsupported event type" *not* `dead`?

Answers are in `scripts/send_webhooks.py` and `tests/test_webhooks.py`: read the test names, they spell out each rule:

| Rule | Where | Test |
|---|---|---|
| Verify HMAC over the **raw bytes**, constant-time compare | `verify_signature` | `test_signature_is_over_raw_bytes_not_reserialised_json`, `test_rejects_bad_or_missing_signature…` |
| Fail closed with no secret configured | `github_webhook` | `test_fails_closed_without_a_configured_secret` |
| Ack fast, process later | `enqueue` only in-request | `test_ack_is_fast_and_duplicates_are_idempotent` |
| Dedupe on delivery id (atomic) | `INSERT OR IGNORE` + PK | same test |
| Out-of-order safe | `WHERE excluded.updated_at >= …` | `test_out_of_order_delivery_cannot_overwrite_newer_state` |
| Poison vs transient errors | `PermanentError` vs retry→`dead` | `test_unknown_events_ignored_poison_parked_ping_ok`, `test_transient_failures_retry_then_park` |
| Recover after an outage | `failed` rows are retried | `test_a_failed_message_succeeds_after_the_outage_ends` |

**Break it:** (a) change `>=` to `<=` in `_apply` and run the out-of-order test; (b) in `github_webhook`, verify against
`json.dumps(json.loads(body)).encode()` instead of `body` and see the pretty-printed-payload test fail — this is
a genuine production bug that makes "random" webhooks fail verification.

**Real-world mapping:** the inbox table is usually Postgres or a queue (SQS, Kafka — see `../kafka-learn`); the worker is
a separate deployment; processed events land in bronze exactly like part 1's pages. Stripe adds a timestamp to the
signature for replay protection; the structure is identical.

## Run the two projects together

```bash
# in api-ingestion-learn:  make mock  /  make run      (builds data/warehouse.duckdb)
DB_PATH=../api-ingestion-learn/data/warehouse.duckdb make serve
```
The API now serves the pipeline's real silver table (same columns). Ingest → warehouse → serve: the full loop.
(Heads-up: DuckDB allows one writer *or* many readers across processes, so run the pipeline, then serve. That limit
is a real reason serving layers usually sit on Postgres/ClickHouse/a warehouse with concurrent readers.)

## What to say in an interview

**Pitch (30 seconds):** *"I built an end-to-end API integration project: an incremental ingestion pipeline with retries,
rate-limit handling, idempotent loads and a data-quality gate, plus the other side: a rate-limited, cursor-paginated
data API and a signed-webhook inbox. Each failure mode has a test I can show."*

Questions you can now answer from experience:
- Offset vs cursor pagination? Why does `OFFSET` duplicate rows? (stage 1 test)
- How do you make a consumer idempotent under at-least-once delivery? (inbox PK + `>=` apply)
- How would you secure a webhook endpoint? (HMAC on raw bytes, constant-time, fail closed, replay window)
- A webhook handler times out and the sender retries forever — what happened and how do you prevent it? (ack fast)
- How do you handle a poison message? (permanent vs transient, dead-letter, alert on count)
- Liveness vs readiness? (`/health` vs `/ready`)
- Token bucket vs fixed window? Why per-client? Where does it live in production?

## Honest limits

- Static API keys and in-memory rate limiting are teaching-grade. Production: OAuth2/OIDC, a gateway, Redis.
- SQLite inbox = single node. At scale: Postgres/queue, multiple workers (`SELECT … FOR UPDATE SKIP LOCKED`), metrics.
- No pagination `total` count or sort options on purpose (counts are expensive on big tables; mention it if asked).
- Not deployed. A natural next step: Dockerfile + GitHub Actions running `make test` for both projects.

## Exercises

1. Add `GET /v1/metrics/daily` backed by `gold.daily_issue_activity` with `from`/`to` date filters and tests.
2. Add an `Idempotency-Key` header to a `POST /v1/exports` endpoint (Stripe-style): same key → same response, no second job.
3. Add `scopes` to API keys (`read`, `admin`) so `/v1/admin/inbox` returns 403 for read-only keys (401 vs 403!).
4. Write webhook payloads to part 1's bronze Parquet layout so webhook events feed the same silver upsert.
5. Add Prometheus metrics: requests by status, inbox depth, age of oldest unprocessed message. Alert rule: depth > N for 5m.
6. Replace the in-memory token bucket with Redis (`INCR` + `EXPIRE`, or a Lua script) and test with two app instances.
