# Lesson 12: Serving data through an API

**Time:** 60 min · **Needs:** Lessons 3, 5, 8 · **Run:** `python api-course/12_serving/demo.py`
**Full project for this lesson:** [`api-serving-learn`](../../api-serving-learn/README.md), stages 1-2

## The problem

Data engineers don't only consume APIs. Once your warehouse has clean data, other teams and applications want it: a product page showing recent orders,
a partner needing a feed, an ML service fetching features. Giving everyone warehouse credentials is unsafe and unmanageable, so you expose a **data API**.
Building one is also the best way to understand the clients' side: you'll write the `429`, the `Link` header and the `ETag` that you spent lessons 3-8 reading.

## Four decisions that matter

### 1. Pagination: cursors beat offsets

```
OFFSET: page 2 repeats 1 row(s) from page 1  [1000063]      <- a new row arrived between the two requests
CURSOR: page 2 repeats 0 row(s)
the cursor is just the last row's sort key: {"u": "2026-09-23T21:00:00", "i": 1000063}
```

`OFFSET 10` means "skip ten rows", so a row inserted above you shifts everything. A cursor says "give me rows **after this one**": `WHERE (updated_at, id) < (?, ?)`. It stays correct as data changes and is
**faster**, since the database seeks via an index instead of counting and discarding N rows (at page 10,000, OFFSET reads 500,000 rows to throw away).

The cursor needs a **unique tie-breaker** (`id`): many rows share a timestamp, and without it, rows at page boundaries get skipped or repeated. Make the cursor opaque (base64) so you can change its internals later.

Also: **cap page size** (`limit <= 200`). An unbounded `limit` is a self-inflicted outage.

### 2. Authentication and authorization

API keys stored as **hashes** (like passwords, so a leaked database doesn't leak keys). `401` for unknown caller. `403` if known but not permitted (scopes: read vs admin).
In companies this is usually delegated: an **API gateway** (Kong, Apigee, AWS API Gateway) plus OAuth2/JWT from an identity provider. The concepts are the same.

### 3. Rate limiting: the token bucket

Each client gets a bucket of tokens (burst size) refilling at a steady rate. Each request spends one token; empty bucket ⇒ `429` + `Retry-After`.

```
  analytics request 3: 200  remaining=0
  analytics request 4: 429  remaining=0  Retry-After=1s
  bi (another client) : 200 <- its own bucket
  analytics, 1s later : 200 <- one token refilled
```

Per-client buckets stop one noisy consumer starving the rest. In-process memory only works for a single server instance. With several replicas you need a shared store (Redis) or a gateway.
Send `X-RateLimit-*` headers on every response so well-behaved clients can slow down on their own: they are the headers lesson 5's client read.

### 4. Conditional responses

Hash the response body into an `ETag`; if the client's `If-None-Match` matches, return `304` with no body. Lesson 8 from the other side:
`first call 200 (1271 bytes); repeat with If-None-Match -> 304 (0 bytes)`.

## Operational endpoints

| Endpoint | Question it answers | What it checks |
|---|---|---|
| `/health` (liveness) | Is the process alive? | Nothing external. If it checked the DB, a DB blip would make Kubernetes restart every pod |
| `/ready` (readiness) | Can it serve right now? | The DB connection. Failing removes the pod from the load balancer without restarting it |

Neither requires an API key: probes and load balancers can't send them.

## Security reminders for any data API

- **Bound parameters only** (`?` placeholders), never string-formatted SQL. The code tests injection strings.
- Return clean `4xx` errors for bad input (bad cursor ⇒ `400`, out-of-range limit ⇒ `422`), never a stack trace or a `500`.
- Don't expose internal columns by accident: use explicit response models/column lists.

## Run it

**Predict:** at which request does the bucket of 3 run dry? What happens to a second client at that moment?

## Honest note on the technology

DuckDB lets one process hold a read-write connection or other processes read, but not both at once. That's why the demo (and the API code) opens a connection per request and closes it.
Real serving layers sit on Postgres, ClickHouse, a warehouse's own SQL API, or a published snapshot, because they need many concurrent readers while a pipeline loads.

## Where it is in the repo

`api-serving-learn/app/pagination.py`, `app/security.py` (token bucket, key check, ETag), `app/main.py` (`list_issues`, `/health`, `/ready`),
`tests/test_read_api.py`.

## Try it yourself

1. Request `limit=200` and `limit=201`. What status do you get for each?
2. Remove the `id` from the cursor condition (keep only `updated_at`) and run the project's tie test. What breaks?
3. Add a `GET /v1/issues/count` endpoint. Why do real APIs often avoid returning total counts on large tables?

## Check yourself

1. Why does offset pagination get slower on deep pages and unreliable on changing data?
2. What's the difference between `/health` and `/ready`?
3. Why store API keys hashed?
4. Why do you need a tie-breaker column in a cursor?

<details><summary>Answers</summary>

1. The database must count and discard N rows; and inserts/deletes above your position shift all later rows between requests.
2. Health: process alive, no dependencies. Ready: dependencies reachable, safe to receive traffic.
3. So a leaked database doesn't expose usable keys (same reasoning as password hashing).
4. Sort keys aren't unique (many equal timestamps); without a unique tie-breaker the "after this row" condition skips or repeats rows at page boundaries.
</details>

**Next:** [Lesson 13: Interview practice](../13_interview/README.md)
