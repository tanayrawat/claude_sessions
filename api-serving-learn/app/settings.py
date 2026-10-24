from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path


def hash_key(api_key: str) -> str:
    """LEARN: never store API keys in plaintext. Store a hash; hash what the client sends and compare.
    (A fast SHA-256 is fine for *high-entropy random* keys. Human passwords need bcrypt/argon2.)"""
    return hashlib.sha256(api_key.encode()).hexdigest()


@dataclass(frozen=True)
class Settings:
    db_path: Path = Path("data/warehouse.duckdb")  # the warehouse we SERVE (part 1's output, or the seeded demo)
    inbox_path: Path = Path("data/inbox.sqlite")  # our own small operational store for webhooks
    webhook_secret: str | None = None
    api_key_hashes: dict[str, str] = field(default_factory=dict)  # sha256(key) -> client name
    rate_capacity: int = 30  # burst size
    rate_refill_per_sec: float = 1.0  # sustained rate
    max_attempts: int = 3  # webhook processing retries before a message is parked as 'dead'

    @classmethod
    def from_env(cls) -> "Settings":
        # API_KEYS="analytics:abc123,bi:def456"  -> two clients, each with its own rate-limit bucket
        pairs = [p.split(":", 1) for p in os.getenv("API_KEYS", "demo:demo-key").split(",") if p]
        return cls(
            db_path=Path(os.getenv("DB_PATH", "data/warehouse.duckdb")),
            inbox_path=Path(os.getenv("INBOX_PATH", "data/inbox.sqlite")),
            webhook_secret=os.getenv("WEBHOOK_SECRET"),
            api_key_hashes={hash_key(k): name for name, k in pairs},
            rate_capacity=int(os.getenv("RATE_CAPACITY", 30)),
            rate_refill_per_sec=float(os.getenv("RATE_REFILL_PER_SEC", 1.0)),
        )
