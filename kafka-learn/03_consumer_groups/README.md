# Stage 3 — Consumer Groups

## Concepts

- Within **one consumer group**, each partition is consumed by exactly one member at a time —
  the group divides the work. More consumers than partitions means some sit idle.
- **Different groups** are fully independent — each gets its own copy of the whole stream. This
  is the pub/sub side of Kafka; the queue-like load-sharing only happens *within* a group.
- A **rebalance** happens whenever a consumer joins or leaves a group (including crashing).
  Partitions get revoked from current members and reassigned, possibly to different consumers.

## Run it

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders.groups --partitions 4 --replication-factor 1

python 03_consumer_groups/producer.py             # terminal A -- leave running
python 03_consumer_groups/consumer.py --name c1   # terminal B
```

## What to notice

1. With only `c1` running, watch it get assigned **all 4** partitions.
2. Now start a second one in a third terminal:
   ```bash
   python 03_consumer_groups/consumer.py --name c2
   ```
   Watch **both** terminals print a `REVOKED` line, then new `ASSIGNED` lines — that's the
   rebalance. `c1` and `c2` now each own roughly half the partitions, and between them they
   process every message exactly once (no duplication across the group).
3. Start a **third** consumer with `--name c3`. Rebalance happens again; work now splits 3 ways.
4. Start a **fourth**, `--name c4` — it gets assigned zero partitions (there are only 4, and 4
   are already owned) and sits idle. More consumers than partitions doesn't buy you more
   parallelism; it just means some consumers do nothing.
5. Kill `c2` with `Ctrl+C`. Another rebalance fires and its partitions get picked up by the
   survivors (including the previously idle `c4`, if it's running).

## Try this

Start a consumer with `--group other-team` while the rest are still running. It gets its own
full assignment of all 4 partitions and reprocesses the entire stream from `earliest`,
completely independent of the `order-processors` group's progress.
