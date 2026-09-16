"""Stage 4: feeds orders.delivery -- run once, leave it running for the rest of this stage."""
import json
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

sys.path.append(str(Path(__file__).resolve().parents[1]))
from common.events import make_event  # noqa: E402

TOPIC = "orders.delivery"


def main():
    producer = Producer({"bootstrap.servers": "localhost:19092"})
    print("producing forever -- Ctrl+C to stop")
    try:
        while True:
            event = make_event()
            producer.produce(
                TOPIC,
                key=event.order_id.encode("utf-8"),
                value=json.dumps(event.to_dict()).encode("utf-8"),
            )
            producer.poll(0)
            time.sleep(0.3)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
