"""Helpers shared by every lesson demo. You don't need to read this file to follow the course."""
from __future__ import annotations

import logging
import sys
import textwrap
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "api-ingestion-learn"))  # gives the demos the `ghpipe` package + the mock GitHub server

from ghpipe.mock_server import MockGitHub  # noqa: E402

# the client logs every retry; show those lines in order with the lesson text, clearly labelled
logging.basicConfig(level=logging.WARNING, stream=sys.stdout, format="    [client log] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)

TOKEN = "dev-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
PATH = "/repos/acme/payments/issues"


def use_serving_project() -> None:
    """Make `app` (the part-2 FastAPI project) importable. Only lessons 11-12 need this."""
    sys.path.insert(0, str(REPO / "api-serving-learn"))


def start_server(**kw) -> MockGitHub:
    """A fake GitHub that runs inside this Python process. Same seed every time => same data every time."""
    return MockGitHub(token=TOKEN, **kw).start()


def title(text: str) -> None:
    print("\n" + "=" * 72 + f"\n{text}\n" + "=" * 72)


def step(text: str) -> None:
    print(f"\n--- {text}")


def say(text: str) -> None:
    print(textwrap.fill(text, 76, initial_indent="  ", subsequent_indent="  "))


def mask(secret: str) -> str:
    return secret[:10] + "…(hidden)"
