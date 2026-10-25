# Lesson 11: Webhooks (push instead of pull)

**Time:** 60 min · **Needs:** Lessons 6, 7 · **Run:** `python api-course/11_webhooks/demo.py`
**Full project for this lesson:** [`api-serving-learn`](../../api-serving-learn/README.md), stage 3

## The problem

Polling has costs: latency (you learn about a change at the next poll), wasted calls (most polls find nothing), and rate limits.
Many providers offer the reverse: you register a URL, and **they send you an HTTP request when something happens**. That's a **webhook**.
Payment succeeded, issue closed, order shipped: you hear about it within seconds.

But now *you* are the server, and the sender's behaviour is hostile in several ways. They will deliver the same event more than once, in
the wrong order, and they expect an answer within seconds or they'll retry. Anyone on the internet can also send requests to your URL pretending to be them.

## What a reliable webhook receiver must do

```
 sender ──POST──▶ [1 verify signature] ─▶ [2 store raw event, dedupe on delivery id] ─▶ [3 answer 202 FAST]
                                                            │
                                                  worker, separately:  [4 apply, order-safe, idempotent]
                                                         pending ─▶ processed
                                                                 ─▶ failed (retry) ─▶ dead (parked)
                                                                 ─▶ ignored (valid event we don't handle)
```

### 1. Verify the signature (security)

The provider signs the request body with a secret you both know (HMAC-SHA256) and puts it in a header (`X-Hub-Signature-256`). You recompute and compare.
Without this, anyone who learns your URL can feed fake events into your data. Two classic mistakes:

- Verifying **parsed-then-re-serialised** JSON. Whitespace and key order differ from what was signed, so valid requests fail (demo part 4).
  Always HMAC the **raw bytes** of the request body.
- Comparing with `==`. Use a constant-time compare (`hmac.compare_digest`) so response timing doesn't leak the signature.

Also **fail closed**: if no secret is configured, reject everything (don't accept unsigned traffic). Some providers (Stripe) include a timestamp in the signature so you can reject replays of old, captured requests.

### 2. Store first, process later ("the inbox pattern")

Answer within a few seconds: GitHub allows ~10, others less, and if you time out they retry (more duplicates). So the request handler does the minimum:
verify, **persist the raw event**, return `202 Accepted`. A separate **worker** processes events from the inbox.

### 3. Deduplicate on the delivery id

Delivery is **at-least-once**: you may receive the same event twice (the sender's retry, a network hiccup). Providers send a unique delivery id header.
`INSERT ... ON CONFLICT DO NOTHING` on a primary key makes the dedupe atomic even if two copies arrive at the same moment. A duplicate gets **`200`** (not an error): from the sender's view, it succeeded, so it stops retrying.

### 4. Assume unordered

Events arrive out of order (retries make an old one arrive after a new one). Apply the same **guard from lesson 7**: only let a newer-or-equal `updated_at` overwrite state.

### 5. Poison messages vs transient failures

| Failure | Example | Action |
|---|---|---|
| **Transient** | Database briefly unavailable | Mark `failed`, retry up to N times, then park |
| **Permanent** | Payload missing the `issue` object | Park as `dead` immediately: retrying can't help |
| **Not for us** | Event type we don't handle | `ignored`. Not an error |

Alert on the count and age of `dead` and unprocessed messages. They're the receiver's equivalent of the pipeline's reject rate.

## Run it

**Predict:** what HTTP status does each of the six deliveries get? After the worker runs, what are the final state and title of issue 42?

```
  issue closed at 11:00                          -> HTTP 202  {'status': 'accepted'}
  SAME delivery again (the sender retried)       -> HTTP 200  {'status': 'duplicate'}
  older edit from 10:00 arrives late             -> HTTP 202  {'status': 'accepted'}
  forged request (wrong secret)                  -> HTTP 401  {'detail': 'bad signature'}
  event type we don't handle                     -> HTTP 202  {'status': 'accepted'}
  broken payload (no 'issue' inside)             -> HTTP 202  {'status': 'accepted'}

  inbox status counts: {'pending': 4}            <- the requests only stored events
  worker result: {'processed': 2, 'ignored': 1, 'dead': 1}
  issue 42 now: state='closed' title='final title' updated_at=2026-10-01T11:00:00Z   <- late edit lost, correctly
  signature matches the re-serialised JSON   : False
```

## Pull, push or CDC? Choosing

| Approach | Best when | Weakness |
|---|---|---|
| **Polling an API** (lessons 3-8) | Only an API exists; minutes of latency are fine | Quota use, latency, can't see deletes |
| **Webhooks** | Provider supports them; you need low latency | Delivery isn't guaranteed if you're down; needs the inbox design |
| **CDC / log-based replication** (Debezium, Fivetran, DMS) | You own the source *database* | Only for your own systems |
| **Event streams** (Kafka, Kinesis) | High volume, many consumers | More infrastructure (see `kafka-learn/`) |

Companies often **combine** polling and webhooks: webhooks for speed, plus a periodic poll or reconciliation to catch events lost while the receiver was down.

## In real jobs

- The inbox is usually Postgres or a message queue (SQS, Pub/Sub, Kafka); the web endpoint is tiny; workers scale separately.
- Local development: tools like `ngrok` or the provider's CLI forward real webhooks to your laptop.
- Webhooks often feed the *same* bronze/silver layers as batch pulls (the raw event lands in bronze).

## Where it is in the repo

`api-serving-learn/app/main.py` → `github_webhook()` and `verify_signature()`; `app/inbox.py` (store, dedupe, process, dead-letter);
`tests/test_webhooks.py` (every rule above has a test).

## Try it yourself

1. In `inbox._apply`, change `>=` to `<=` and re-run the demo. What's the final state of issue 42?
2. Send the same payload with a *new* delivery id. Is it a duplicate? What does that mean for providers that reuse payloads?
3. Make the worker crash halfway (raise on the 2nd message). Re-run it. Which messages are re-processed and is the result correct?

## Check yourself

1. Why return `202` quickly instead of processing in the request?
2. What do you return for a duplicate delivery, and why not an error?
3. Why must the signature be verified over raw bytes?
4. A message fails every time with a missing field. Retry it 10 times?

<details><summary>Answers</summary>

1. The sender times out and retries if you're slow, creating more duplicates and load; persisting is fast and safe, and processing can fail and be retried independently.
2. `200` (or 2xx): the sender needs a success to stop retrying; the event is already safely stored.
3. The signature is computed over exact bytes; parsing and re-serialising changes whitespace/ordering and breaks the match.
4. No: it's a permanent failure. Park it as dead immediately and alert.
</details>

**Next:** [Lesson 12: Serving data through an API](../12_serving/README.md)
