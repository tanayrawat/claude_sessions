# Learning Kafka by Building Something

Six small, runnable stages, each adding exactly one Kafka concept on top of the last. Every
stage is real code against a real (local) broker — nothing here is a slide.

```
                     ┌─────────────────────────────┐
                     │           Kafka             │
producer.py  ──────▶ │  a topic, 3 partitions        │
                     │   ┌───────────┐               │
                     │   │partition 0│ offset 0,1,2… │
                     │   └───────────┘               │
                     │   ┌───────────┐               │
                     │   │partition 1│ offset 0,1,2… │──────▶ consumer.py
                     │   └───────────┘                        (group: readers)
                     │   ┌───────────┐               │
                     │   │partition 2│ offset 0,1,2… │
                     │   └───────────┘               │
                     └─────────────────────────────┘
```

A **topic** is a durable, append-only log split into **partitions** for parallelism. A
**producer** appends; a **consumer group** tracks, per partition, how far it has read. That's
the entire mental model everything else in this repo builds on.

## Prerequisites

- Docker + Docker Compose
- Python 3.10+

## Setup

```bash
cd kafka-learn
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

make up       # starts Kafka (KRaft mode, no Zookeeper), Schema Registry, and Kafka UI
make topics   # creates every topic every stage below needs, once
```

- Kafka UI (browse topics/partitions/consumer groups visually): http://localhost:8080
- Schema Registry REST API: http://localhost:8081

When you're done: `make down` (or `make clean` to also drop the volume and start fully fresh
next time).

## Learning path

Work through these in order — each stage's README opens with the concept, then a "what to
notice" section, then a "try this" exercise that's more instructive than just running the happy
path once.

| Stage | Concept | Folder |
|---|---|---|
| 1 | Topics, partitions, offsets, consumer groups | [`01_hello_kafka/`](01_hello_kafka/) |
| 2 | Keys, partitioning, per-key ordering | [`02_keys_and_partitions/`](02_keys_and_partitions/) |
| 3 | Consumer groups scaling out, rebalancing | [`03_consumer_groups/`](03_consumer_groups/) |
| 4 | At-most/at-least/exactly-once delivery, transactions | [`04_delivery_semantics/`](04_delivery_semantics/) |
| 5 | Schema Registry, Avro, backward-compatible evolution | [`05_schema_registry_avro/`](05_schema_registry_avro/) |
| 6 | Windowed aggregation, dead-letter queues | [`06_stream_processing_dlq/`](06_stream_processing_dlq/) |

Stage 4 is the one worth slowing down for — it's the single most common Kafka interview topic,
and the only way to actually understand it is to watch a crash produce a duplicate, then watch
a transaction refuse to let that happen.

## Troubleshooting

- **Port already in use** — the broker publishes on host port **19092** (not the usual 9092)
  specifically so it doesn't collide with another local Kafka you might already be running; if
  19092, 8080, or 8081 are *also* taken, edit the port mappings in `docker-compose.yml`. If you
  change 19092, remember `KAFKA_ADVERTISED_LISTENERS`' `PLAINTEXT_HOST` value has to match it
  exactly (that's what tells host-side clients where to reconnect), and every script's
  `bootstrap.servers` needs to match too.
- **Connection refused right after `make up`** — the broker takes a few seconds to finish
  startup; `make up` waits for its healthcheck, but if you skipped straight to a script, just
  retry, or check `make logs`.
- **`UNKNOWN_TOPIC_OR_PART` errors** — you skipped `make topics`, or a stage's README asks you
  to create one more specific to it (auto-topic-creation is intentionally disabled in
  `docker-compose.yml` so you always create topics on purpose, with partition counts you chose).

## Where to go next

This repo stops at plain Kafka + the Python client. The natural next steps, roughly in order of
how most data engineering roles actually use this:
- **Kafka Streams / ksqlDB / Flink** — real stream processing frameworks that do windowing with
  proper event-time watermarks, joins, and exactly-once state stores, instead of the from-scratch
  approximation in Stage 6.
- **Spark Structured Streaming reading from Kafka into Delta Lake / Unity Catalog** — if you've
  been working through the Unity Catalog material in this same repo's other sessions, this is
  the bridge: `spark.readStream.format("kafka")...writeStream.toTable("catalog.schema.table")`
  lands exactly the kind of events this project generates straight into a governed lakehouse table.
- **A managed Kafka** (Confluent Cloud, Amazon MSK, Azure Event Hubs' Kafka-compatible endpoint)
  — everything you did here with `docker exec kafka kafka-topics` becomes Terraform/CLI/console
  against a multi-broker, multi-AZ cluster with real replication factors.
- **Security** — SASL/SCRAM or mTLS between clients and brokers, and topic-level ACLs; this repo
  runs everything `PLAINTEXT` with no auth because it's local and disposable.
- **Monitoring consumer lag** — `docker exec kafka kafka-consumer-groups --bootstrap-server
  localhost:9092 --describe --group order-processors` shows you the `LAG` column live; in
  production that number, alerted on, is usually your first signal something downstream is stuck.
