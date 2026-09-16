"""Shared fake order-event generator used by every stage in this project.

Keeping this in one place means the only thing that changes between stages
is how events get produced/consumed/processed -- not what they look like.
"""
import random
import time
from dataclasses import asdict, dataclass

ORDER_STATUSES = ["created", "paid", "shipped", "delivered", "cancelled"]


@dataclass
class OrderEvent:
    order_id: str
    status: str
    amount: float
    customer_id: str
    ts: float

    def to_dict(self) -> dict:
        return asdict(self)


def random_order_id(pool_size: int = 50) -> str:
    """A bounded pool of ids, so re-running this many times still produces
    multiple events per order -- which is what makes partitioning-by-key
    (Stage 2) and windowed aggregation (Stage 6) interesting to watch."""
    return f"order-{random.randint(1, pool_size):04d}"


def make_event(order_id: str | None = None, status: str | None = None) -> OrderEvent:
    return OrderEvent(
        order_id=order_id or random_order_id(),
        status=status or random.choice(ORDER_STATUSES),
        amount=round(random.uniform(5, 500), 2),
        customer_id=f"cust-{random.randint(1, 20):03d}",
        ts=time.time(),
    )
