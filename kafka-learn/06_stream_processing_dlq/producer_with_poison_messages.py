"""Stage 6: like Stage 1's producer, but ~1 in 8 messages is deliberately corrupt --
simulating a buggy upstream service so we have something for the DLQ pattern to catch."""
import json
import random
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

sys.path.append(str(Path(__file__).resolve().parents[1]))
from common.events import make_event  # noqa: E402

TOPIC = "orders.dirty"


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    print("producing (with occasional poison messages) -- Ctrl+C to stop")
    try:
        while True:
            if random.randint(1, 8) == 1:
                value = b"{not-valid-json::"
            else:
                value = json.dumps(make_event().to_dict()).encode("utf-8")
            producer.produce(TOPIC, value=value)
            producer.poll(0)
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
