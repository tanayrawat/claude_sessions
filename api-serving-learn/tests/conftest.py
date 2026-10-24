import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.settings import Settings, hash_key
from scripts.seed_demo_db import seed

KEY = {"X-API-Key": "k-analytics"}


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def settings(tmp_path):
    seed(tmp_path / "wh.duckdb", n=120)
    return Settings(
        db_path=tmp_path / "wh.duckdb", inbox_path=tmp_path / "inbox.sqlite", webhook_secret="s3cret",
        api_key_hashes={hash_key("k-analytics"): "analytics", hash_key("k-bi"): "bi"},
        rate_capacity=1000, rate_refill_per_sec=1000,  # effectively unlimited unless a test overrides
    )


@pytest.fixture
def client(settings, clock):
    return TestClient(create_app(settings, clock))
