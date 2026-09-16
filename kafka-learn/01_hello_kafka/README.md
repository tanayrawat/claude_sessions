# Stage 1 — Hello Kafka

## Concepts

- **Broker** — the Kafka server itself; ours is the single-node `kafka` container.
- **Topic** — a named, append-only log. `orders.v1` here.
- **Partition** — a topic is split into 1+ partitions, each an independent ordered log. This
  topic has exactly **1**, so right now the whole topic is one strictly-ordered sequence.
- **Offset** — a message's position within its partition. Monotonically increasing, never reused.
- **Consumer group** — a named set of consumers (`group.id`) that share a subscription. Kafka
  remembers, per group, how far it has read (its *committed offset*).

## Run it

```bash
# from kafka-learn/, with the stack already up (docker compose up -d)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders.v1 --partitions 1 --replication-factor 1

# terminal A
python 01_hello_kafka/consumer.py

# terminal B
python 01_hello_kafka/producer.py
```

## What to notice

- Every message the consumer prints has an **increasing offset**, in the exact order the
  producer sent them — guaranteed only because there's one partition.
- Stop the consumer (`Ctrl+C`) and start it again: it **resumes from where it left off**, it
  doesn't reprint old messages. That's the consumer group's committed offset at work — by
  default this client auto-commits periodically as you consume.
- Delete the `group.id` and pick a new one (or use `kafka-consumer-groups --reset-offsets`) and
  it reads everything again from `earliest`. The offset lives with the *group*, not the topic.

## Try this

Run the consumer with a *new* `group.id`, in parallel with the original one, while the producer
is still running. Both groups get a full, independent copy of every message — this is the
pub/sub half of Kafka's personality. (Stage 3 covers the other half: consumers *within* the
same group split the work instead of duplicating it.)
