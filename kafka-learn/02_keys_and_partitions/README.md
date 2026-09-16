# Stage 2 — Keys & Partitions

## Concepts

Kafka only guarantees ordering **within a partition**, never across an entire topic. The default
partitioner hashes the message **key** to pick a partition — so the way to get "all events for
order X arrive in order" is to key every event for that order with the same key. No key means
the client spreads messages across partitions (sticky/round-robin batches), which is great for
throughput and useless for ordering.

## Run it

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders.keyed --partitions 3 --replication-factor 1

python 02_keys_and_partitions/consumer.py     # terminal A
python 02_keys_and_partitions/producer.py     # terminal B
```

## What to notice

- The producer round-robins through 6 order ids, but the consumer output shows each
  `order-XXXX` key **always** landing on the same partition, every time it recurs.
- Within one partition, that order's events print in the same relative order the producer sent
  them, even though partition assignment interleaves them across the terminal output.

## Try this — and the trap

Delete the topic and recreate it with **6** partitions instead of 3, then run the producer again
against the same `order-XXXX` ids used before.

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic orders.keyed
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders.keyed --partitions 6 --replication-factor 1
```

Notice a key can land on a **different** partition than it did before — `hash(key) % partition_count`
changes when `partition_count` changes. This is exactly why **you don't repartition a topic that
existing keyed data depends on for ordering** without a migration plan; it's one of the most
common "why did our ordering guarantee silently break" incidents in production Kafka.
