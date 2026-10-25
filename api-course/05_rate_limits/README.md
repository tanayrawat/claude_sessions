# Lesson 5: Rate limits

**Time:** 45 min · **Needs:** Lessons 1-4 · **Run:** `python api-course/05_rate_limits/demo.py` (about 15 seconds: it really waits)

## The problem

Providers protect themselves and other customers by capping how fast you may call. GitHub gives an authenticated user 5,000 requests per hour.
Many SaaS APIs allow only a few requests *per second*. Exceed the cap and you get errors, temporary blocks, or in the worst case a banned account or a bill.
A pipeline backfilling two years of data will hit the limit within minutes unless it's designed for it.

## The idea: read the budget, spend it, wait when it's empty

Providers expose your budget in response headers (names vary, GitHub's are typical):

| Header | Meaning |
|---|---|
| `X-RateLimit-Limit` | Your total allowance per window (e.g. 5000) |
| `X-RateLimit-Remaining` | How many requests you have left in this window |
| `X-RateLimit-Reset` | When the window resets (a Unix timestamp, i.e. seconds since 1970) |
| `Retry-After` | Seconds to wait (sent with 429/503) |

### Reactive vs proactive

- **Reactive:** send requests until you get rejected, then wait. Works, but you collect errors (some providers count them against you, or
  escalate to bans for repeated violations).
- **Proactive:** after each response check `Remaining`. When it reaches your safety margin (1 or 2), sleep until `Reset`. You never see an error.

### The status-code trap

Most APIs signal a limit with `429`. **GitHub's primary limit uses `403`** with `Remaining: 0`. A client that retries only on `429` and fails hard on `403`
breaks. Another that retries *every* `403` hammers a real "forbidden" forever. So the right rule is: `403` **and** `Remaining == 0` ⇒ rate limit.

### Fail fast on long waits

If the reset is 40 minutes away, do you sleep for 40 minutes inside the task? No: that holds a worker slot idle, the scheduler can't tell "busy" from
"stuck", and your task may exceed its timeout anyway. Better: **if the wait exceeds a threshold, raise an error** (`RateLimitExceeded`) and let the
orchestrator reschedule after the reset. Your monitoring will also see that this happens, and you can ask the provider for a higher quota or spread the work out.

### Be a good citizen (design choices beyond the client)

- Prefer **incremental loads** (lesson 6) and **conditional requests** (lesson 8): the best way to respect a limit is to need fewer calls.
- Use **bulk endpoints** if offered (one call for 1,000 records instead of 1,000 calls).
- If you run several workers, they **share one budget**. Concurrency needs a shared limiter (e.g. a token bucket in Redis).
- Spread large backfills over time instead of bursting.

## Run it

**Predict:** with a limit of 5 requests per 3 seconds, which request first fails, and with what status?

```
  request 1: status=200  remaining=4  limit=5
  ...
  request 5: status=200  remaining=0  limit=5
  request 6: status=403  remaining=0  limit=5        <- 403, not 429!
  ...
  fetched 250 rows in 9.7s using 9 requests
  paused 2 time(s) to wait for the quota to reset; errors: 0     <- slowed down but never got an error
  raised RateLimitExceeded: would need to wait 3602s (max_wait is 5s)
```

## In real jobs

- You'll read provider docs for limit details: per second vs per hour, per token vs per IP, separate limits for search endpoints (often much lower), and
  *secondary/abuse* limits for bursts or concurrency.
- Rate limits are a top reason a backfill takes days. Estimate it up front: `rows / page_size = requests`; `requests / limit_per_hour = hours`.
- Multi-tenant pipelines (one job per customer) often have one token per customer, so budgets are per-customer.

## Where it is in the repo

`ghpipe/client.py` → `_rate_limited()`, `_server_wait()`, `_throttle_if_low()`, `RateLimitExceeded`; tests
`test_proactively_waits_for_quota_reset`, `test_refuses_to_sleep_for_an_hour`. The *server* side of this is lesson 12.

## Try it yourself

1. In part 2, set `min_remaining=0` on the client. What changes? (Hint: you now spend the last request and risk the next one.)
2. Estimate how long a full backfill of 2 million issues takes at 100 per page and 5,000 requests/hour.
3. Run two clients at once against one server with limit 5. What happens to each, and what does that tell you about workers sharing a token?

## Check yourself

1. How do you tell a rate-limit `403` from a permissions `403`?
2. Why prefer sleeping until `Reset` over retrying every second?
3. When should a client raise instead of sleep?
4. Name two ways to need fewer requests.

<details><summary>Answers</summary>

1. Look at `X-RateLimit-Remaining` (0) and the reset header, or the body message. A permissions `403` has budget left.
2. Retrying every second wastes your own requests, may be counted as abuse, and doesn't make the quota come back any sooner.
3. When the required wait exceeds what a task should hold a worker for (a configured max). Raise so the orchestrator can reschedule.
4. Incremental loads (only changes), conditional requests (304s), bulk endpoints, larger page sizes, caching, deduplicating requests.
</details>

**Next:** [Lesson 6: Incremental loading](../06_incremental/README.md)
