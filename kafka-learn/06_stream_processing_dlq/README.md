# Stage 6 — Stream Processing & Dead-Letter Queues

Two independent, small patterns that show up in almost every real Kafka consumer.

## Part A — Windowed aggregation

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --topic revenue.windowed --partitions 1 --replication-factor 1

# reuses orders.delivery from Stage 4 -- start that stage's producer if it isn't running
python 04_delivery_semantics/producer.py &

python 06_stream_processing_dlq/windowed_aggregator.py
```

Watch it print a closed window roughly every 10 seconds: an order count and total revenue for
`"paid"` events in that bucket. Read the module docstring — it's deliberately naive about
late-arriving events, on purpose, and says exactly what a real framework (Kafka Streams, Flink,
Structured Streaming) adds on top: watermarks, so a message that arrives slightly late for its
window still gets counted correctly instead of silently missed.

## Part B — Dead-letter queue

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --topic orders.dirty --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --topic orders.dirty.dlq --partitions 1 --replication-factor 1

python 06_stream_processing_dlq/producer_with_poison_messages.py   # terminal A
python 06_stream_processing_dlq/dlq_consumer.py                    # terminal B
```

## What to notice

The consumer keeps running through every poison message — it never crashes, never blocks the
partition, and every good message on either side of a bad one still gets processed. Inspect
what landed in the DLQ, headers and all:

```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic orders.dirty.dlq --from-beginning --property print.headers=true --timeout-ms 5000
```

## Try this

This DLQ consumer commits offsets automatically (default `enable.auto.commit`) whether a
message succeeded or was quarantined — that's the right call for a poison *message* (permanently
unparseable, retrying won't help). Now imagine the failure is a downstream dependency being
temporarily down instead (a database write failing) rather than bad data. Sketch how you'd
change this: you'd want retries with backoff before giving up and routing to a DLQ, and you'd
want to make sure you don't advance the offset while retries are still in flight. This exact
distinction — "poison message, DLQ immediately" vs "transient failure, retry then DLQ" — is a
favorite interview follow-up to "how do you handle errors in a Kafka consumer?"
