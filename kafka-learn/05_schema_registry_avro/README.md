# Stage 5 — Schema Registry & Avro Evolution

## Concepts

- Kafka messages are just bytes; it doesn't know or care about your schema. **Schema Registry**
  is a separate service that stores schemas by (subject, version) and lets producers/consumers
  agree on structure without hardcoding it — the wire format is a 5-byte prefix (magic byte +
  schema id) followed by the Avro-encoded payload.
- A schema evolves **backward-compatibly** when new-schema data can still be read by an
  old-schema reader (e.g. adding a field *with a default*, so an old reader that doesn't know
  about it just... doesn't see it, and a `null`/default fills in for anyone reading old data
  with the new schema). This is the default (and recommended) compatibility mode.
- The registry **rejects** a schema registration that breaks the configured compatibility rule —
  it's a build-time-ish guardrail against shipping a breaking change to a shared topic.

## Run it

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --topic orders.avro --partitions 3 --replication-factor 1

python 05_schema_registry_avro/consumer_avro.py                          # terminal A, leave running
python 05_schema_registry_avro/producer_avro.py --schema order_v1.avsc   # terminal B
```

## What to notice

The consumer prints `currency=<not in this message's schema>` for every v1 message — it's
reading the schema the *producer* actually used, not a schema it hardcoded itself.

## Try this — the evolution

Without touching `consumer_avro.py` at all, run the v2 producer:

```bash
python 05_schema_registry_avro/producer_avro.py --schema order_v2.avsc
```

Watch the same running consumer start printing `currency=USD` for the new messages,
side-by-side with the old `currency=<not in this message's schema>` ones for messages
produced earlier — one consumer, two schema versions, zero code changes, zero downtime.

Now open the Schema Registry's REST API directly and look at what got registered:

```bash
curl -s localhost:8081/subjects/orders.avro-value/versions | jq
curl -s localhost:8081/subjects/orders.avro-value/versions/2 | jq
```

## The trap

Edit `order_v2.avsc` to make `currency` **required** (remove `"default": "USD"`) and try
producing with it against the same subject. Schema Registry will reject the registration under
`BACKWARD` compatibility (the default): an old consumer reading old data has no way to invent a
value for a newly-required field, so the registry refuses to let that schema in at all. This is
the exact mechanism that stops "someone added a required field and broke every downstream
consumer" from ever reaching production.
