# Lesson 4: Failures and retries

**Time:** 60 min · **Needs:** Lessons 1-3 · **Run:** `python api-course/04_retries/demo.py`

## The problem

A pipeline makes thousands of calls a day to systems you don't control. Some will fail: a deploy on their side, a
load balancer blip, a dropped connection. Most of these failures **fix themselves within seconds**. A pipeline that gives up at the
first failure pages someone at 3am for nothing. A pipeline that retries *badly* can make a struggling server worse, or retry things that can
never work. Getting retries right is one of the clearest signals of engineering maturity.

## The idea: retry only what can recover, wait smartly, and eventually stop

### 1. Decide what deserves a retry

| Response | Retry? | Why |
|---|---|---|
| Network error, timeout | Yes | Connection trouble is usually temporary |
| `500`, `502`, `503`, `504` | Yes | The server is struggling or restarting |
| `429 Too Many Requests` | Yes, after the wait they specify | You're sending too fast |
| `408 Request Timeout` | Yes | Same family as above |
| `403` with "rate limit" headers | Yes, after the reset | Some APIs (GitHub) report an exhausted quota this way |
| `400`, `401`, `404`, `422`, other `403` | **No** | *Your* request is wrong. The same request fails forever. Stop and alert |

Retrying a `404` five times just wastes a minute before failing the same way. Retrying a `401` can get your account locked.

### 2. Wait longer each time: exponential backoff

If the server is overloaded, immediate retries make it worse. After each failure wait about **2x as long**: 0.5s, 1s, 2s, 4s...
up to a cap (e.g. 30s). This is *exponential backoff*.

### 3. Add randomness: jitter

Suppose 2,000 workers all hit an outage at the same moment. With plain backoff, all of them retry at *exactly* 1s, then *exactly* 2s...
The server recovers a little and gets hit by 2,000 requests at once, falls over again. This is a **retry storm** (or "thundering herd").
**Full jitter** picks a random wait between 0 and the backoff value, spreading the retries out. The demo's histogram shows it: the same
2,000 clients produce a peak of 2,000 requests in one 0.1s window without jitter, and about 215 with it.

### 4. Obey the server

If the response says `Retry-After: 30`, the server is telling you precisely when to come back. Use that instead of your own guess.

### 5. Give up eventually

After N attempts, raise a clear error so the orchestrator (Airflow, etc.) marks the task failed and its own retry policy
and alerting take over. An infinite retry loop hides an outage.

### Two layers of retry, not one

| Layer | Handles | Example |
|---|---|---|
| **Request-level** (inside your client) | Brief blips lasting seconds | This lesson's client |
| **Task-level** (the orchestrator) | Longer problems: the API is down for 10 minutes | Airflow `retries=3, retry_delay=5min` |

The task-level retry is only safe because your load is idempotent (lesson 7); otherwise re-running a half-finished task duplicates data.

## Run it

**Predict:** both callers face the same three failures (500, 503, 429-with-Retry-After: 1). What happens to each?
What wait does the robust client pick for the 429?

```
  naive caller  : crashed on the first blip -> 500
    [client log] retryable failure (500) ...; attempt 1, sleeping 0.12s
    [client log] retryable failure (503) ...; attempt 2, sleeping 0.10s
    [client log] retryable failure (429) ...; attempt 3, sleeping 1.00s     <- exactly the server's Retry-After
  robust caller : succeeded, got 100 rows after 3 retries
  ...
  Busiest 0.1s for the struggling server: 2000 requests vs 215.
  after 3 retries: RetriesExhausted
```

(The first two waits are random, so yours will differ. The 1.00s will not.)

## In real jobs

- Libraries exist: `tenacity` (generic), `urllib3.Retry` / `requests.adapters.HTTPAdapter`, `httpx` transports, and cloud SDKs ship with
  retry built in. In an interview you're expected to know *what* they do and which settings matter, not to hand-write it.
- **Retry only idempotent requests.** Retrying a `GET` is always safe. Retrying a `POST` that creates something might create it twice unless the API
  supports an *idempotency key* (Stripe does).
- Log every retry with the reason and wait time (the demo does). "Retries went from 2% to 40% yesterday" is how you notice a provider degrading *before* they announce it.
- Set **timeouts** (connect and read), else a hung connection never fails and never retries.

## Where it is in the repo

`api-ingestion-learn/ghpipe/client.py` → `get()` (the retry loop), `_backoff()`, `_server_wait()`; tests `test_retries_5xx_then_succeeds`,
`test_backoff_is_jittered_and_capped`, `test_honours_retry_after_on_429`, `test_gives_up_after_max_retries`, `test_does_not_retry_client_errors`.

## Try it yourself

1. In the demo, add `404` to the retryable set. Time how long a request to a wrong URL now takes to fail. Why is that bad?
2. Set `backoff_cap=0.1`. What does this do to the server under a storm?
3. Make the fault list 6 long with `max_retries=5`. Confirm you get `RetriesExhausted`.

## Check yourself

1. List three conditions you'd retry and three you wouldn't.
2. What problem does jitter solve that exponential backoff alone doesn't?
3. A `POST /payments` times out. Is it safe to retry? What would make it safe?
4. Why have retries both in the HTTP client and in Airflow?

<details><summary>Answers</summary>

1. Retry: timeout, 503, 429. Don't: 401, 404, 422.
2. Synchronisation: many clients that failed together would otherwise retry together, repeatedly. Jitter desynchronises them.
3. Not by default: the first attempt may have succeeded and the response was lost, so you could charge twice. It is safe if the API supports an idempotency key (the same key means the same single effect).
4. They handle different time scales: the client absorbs blips of seconds; the orchestrator handles outages of minutes or more and gives you visibility, alerts and a history of attempts.
</details>

**Next:** [Lesson 5: Rate limits](../05_rate_limits/README.md)
