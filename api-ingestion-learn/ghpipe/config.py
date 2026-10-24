from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    base_url: str = "http://127.0.0.1:8099"
    repo: str = "acme/payments"
    token: str | None = None  # never hardcode; comes from env / secret manager
    data_dir: Path = Path("data")
    per_page: int = 100  # GitHub max
    lookback_seconds: int = 0  # re-read a safety window behind the watermark (late-arriving updates)
    max_reject_rate: float = 0.05  # DQ gate: >5% unparseable records fails the run

    @property
    def db_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def source_key(self) -> str:
        return f"github:{self.repo}:issues"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            base_url=os.getenv("API_BASE_URL", cls.base_url),
            repo=os.getenv("GITHUB_REPO", cls.repo),
            token=os.getenv("GITHUB_TOKEN"),
            data_dir=Path(os.getenv("DATA_DIR", "data")),
            per_page=int(os.getenv("PER_PAGE", cls.per_page)),
            lookback_seconds=int(os.getenv("LOOKBACK_SECONDS", cls.lookback_seconds)),
            max_reject_rate=float(os.getenv("MAX_REJECT_RATE", cls.max_reject_rate)),
        )
