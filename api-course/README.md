# APIs in Data Engineering: a hands-on course

**Start here.** This course takes you from "what is an API call?" to building the kind of ingestion pipeline
data engineers run in production. Every lesson shows a problem **breaking on purpose**, explains why, and then shows the fix.

## Who this is for

You know basic Python and basic SQL. You do **not** need to know HTTP, REST, or anything about pipelines.
If a lesson covers something you already know, skim its "Check yourself" questions; if you can answer them, move on.

## What you will be able to do afterwards

- Explain how data gets from a vendor's API into a warehouse, and where it can go wrong at each step.
- Write an API client that survives timeouts, 500s, rate limits and changing data.
- Build a load that is safe to retry and safe to backfill (idempotent + incremental).
- Receive webhooks and serve your own data API without the classic mistakes.
- Answer interview questions about all of the above from experience rather than memory.

## The path

| # | Lesson | The question it answers | Time |
|---|---|---|---|
| 1 | [What an API call really is](01_http/README.md) | What is a request, a response, a status code, JSON? | 45 min |
| 2 | [Authentication and secrets](02_auth/README.md) | How do I prove who I am, and not leak it? | 30 min |
| 3 | [Pagination](03_pagination/README.md) | How do I get *all* the data, and not silently lose rows? | 60 min |
| 4 | [Failures and retries](04_retries/README.md) | What do I do when a call fails? | 60 min |
| 5 | [Rate limits](05_rate_limits/README.md) | How do I stay inside the provider's budget? | 45 min |
| 6 | [Incremental loading](06_incremental/README.md) | How do I fetch only what changed? | 60 min |
| 7 | [Idempotency](07_idempotency/README.md) | How do I make re-runs safe? | 45 min |
| 8 | [ETags and conditional requests](08_etag/README.md) | How do I poll without wasting my quota? | 30 min |
| 9 | [Raw data and contracts](09_raw_and_contracts/README.md) | How do I handle bad, changing and missing data? | 45 min |
| 10 | [The whole pipeline](10_capstone_pipeline/README.md) | How do all those pieces fit together? | 90 min |
| 11 | [Webhooks](11_webhooks/README.md) | What if the provider calls *me* instead? | 60 min |
| 12 | [Serving data through an API](12_serving/README.md) | What does it look like from the server's side? | 60 min |
| 13 | [Interview practice](13_interview/README.md) | How do I turn this into interview answers? | 90 min |

About 12 hours of focused work. Lessons 1-10 matter most. 11-12 are valuable but less often asked.

## How to study (this matters more than the code)

For **each** lesson, do the loop:

1. **Read** "The problem" and "The idea" in the lesson README. Stop before "Run it".
2. **Predict:** look at what the demo does and write down what you expect it to print.
3. **Run it:** `python api-course/NN_name/demo.py` from the repository root. Compare with your prediction.
4. **Break it:** do the exercise at the end of the lesson (change one line, see what fails).
5. **Close the README** and explain the lesson out loud in under a minute. If you get stuck, re-read that part.

Reading without running, or running without predicting, gives you a lot less. The prediction step is where the learning happens.

## Setup (once, 2 minutes)

```bash
git clone <this repo> && cd claude_sessions
python3 -m venv .venv && source .venv/bin/activate
pip install -r api-course/requirements.txt
python api-course/01_http/demo.py        # should print a lesson with no errors
```

You don't need accounts, API keys, Docker or internet access. All the demos talk to a **fake GitHub server that
runs inside the demo itself**. It behaves like the real GitHub Issues API (pagination, ETags, rate limits, failures)
and you can make it misbehave on purpose.

## How the pieces of this repository fit

```
 api-course/              <- YOU START HERE: 13 short lessons, each with a runnable demo
 api-ingestion-learn/     <- the capstone project: a complete ingestion pipeline (lessons 3-10 combined, + tests)
 api-serving-learn/       <- second project: serve data + receive webhooks (lessons 11-12 in full)
 kafka-learn/             <- separate topic (streaming); not needed for this course
```

The lessons import the real code from `api-ingestion-learn/ghpipe` (the retry client, the mock server, the pipeline), so when you reach
the capstone, you've already seen each of its parts working on its own.

## Vocabulary you'll meet (skim now, refer back later)

| Term | Meaning |
|---|---|
| **Endpoint** | One URL path an API offers, e.g. `/repos/acme/payments/issues` |
| **Payload / body** | The data in a request or response (usually JSON) |
| **Pagination** | Splitting a big result into pages |
| **Rate limit** | The provider's cap on how many requests you may send per time window |
| **Backoff / jitter** | Waiting longer after each failure / adding randomness to the wait |
| **Watermark** | Bookmark: the newest `updated_at` you've loaded so far |
| **Incremental load** | Fetching only data changed since the watermark |
| **Idempotent** | Running twice gives the same result as running once |
| **Upsert** | Insert a row, or update it if it already exists |
| **Bronze / silver / gold** | Raw copy / cleaned & de-duplicated / business-ready aggregates |
| **Dead letter** | Where records that failed validation are kept, with the reason |
| **Webhook** | The provider sending *you* an HTTP request when something happens |
| **At-least-once** | Delivery guarantee: you may get duplicates but never lose a message |
| **ETag** | A fingerprint of a response, used to ask "has this changed?" |
