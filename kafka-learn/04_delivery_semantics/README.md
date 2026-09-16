# Stage 4 — Delivery Semantics

The single most-tested Kafka topic in interviews, and the one people get vague on because
they memorize the terms without ever watching them happen. This stage makes all three real.

## Concepts

- **At-most-once** — commit the offset *before* processing. A crash mid-processing means that
  message is gone forever; it will never be redelivered.
- **At-least-once** — commit the offset *after* processing. A crash between processing and
  committing means that message gets redelivered and reprocessed on restart — a duplicate.
- **Exactly-once** — not a magic setting, but a pattern: an *idempotent* producer (dedupes
  broker-level retries) plus *transactions* that bind "produce the output" and "commit the
  input offset" into one atomic unit, so a crash can't do one without the other.

## Run it

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders.delivery --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders.idempotent --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic revenue.v1 --partitions 3 --replication-factor 1

python 04_delivery_semantics/producer.py   # leave running in its own terminal
```

### 1. At-most-once vs at-least-once

```bash
python 04_delivery_semantics/manual_commit_consumer.py --commit after   # let it print ~8, Ctrl+C mid-batch, rerun
python 04_delivery_semantics/manual_commit_consumer.py --commit before  # same drill
```
Read the module docstring in `manual_commit_consumer.py` before running — it tells you exactly
what to watch for at the crash boundary in each mode.

### 2. Idempotent producer, and its limit

```bash
python 04_delivery_semantics/idempotent_producer.py
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders.idempotent --from-beginning --timeout-ms 3000 2>/dev/null | wc -l   # note the count

python 04_delivery_semantics/idempotent_producer.py --duplicate-bug
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders.idempotent --from-beginning --timeout-ms 3000 2>/dev/null | wc -l   # up by 2x this run's messages
```
`enable.idempotence` stops the *client library* from double-sending during its own retries
(e.g. a request that timed out but actually succeeded). It has no visibility into your
application logic calling `produce()` twice for what you consider one event — that's an
application-level exactly-once problem, and it's why idempotence alone is not "exactly-once."

### 3. Real exactly-once: consume-transform-produce

```bash
python 04_delivery_semantics/transactional_processor.py    # terminal A
python 04_delivery_semantics/read_committed_consumer.py    # terminal B
```
Watch revenue records appear in terminal B only for `"paid"` events, one-for-one, no duplicates
even if you kill and restart `transactional_processor.py` repeatedly.

## What to notice

Stop `transactional_processor.py` mid-stream (`Ctrl+C`) and restart it. It resumes and you'll
neither see a duplicate revenue record for an order already processed, nor a missing one — the
transaction guarantees the produce and the offset commit happened together, or not at all.

## Try this

Change `read_committed_consumer.py`'s `isolation.level` to `read_uncommitted` (the client
default) and force an abort by temporarily breaking `OUTPUT_TOPIC` to a name that doesn't exist
partway through a run. With `read_uncommitted`, you can catch the aborted write in the log
before compaction cleans it up; with `read_committed`, you never see it at all. That difference
is the entire point of the isolation level setting.

**Nuance worth knowing:** because offsets here only get committed as part of a transaction
triggered by a `"paid"` event, a very long run of non-`"paid"` events with no crash-free `"paid"`
event after them won't advance the committed offset. That's harmless here (skipping is
idempotent), but in a real system with a similar shape you'd periodically commit an
empty/no-op transaction just to advance offsets and bound how much gets re-read after a restart.
