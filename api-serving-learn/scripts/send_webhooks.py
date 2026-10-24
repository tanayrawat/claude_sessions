"""Pretend to be GitHub. Sends signed webhooks (and some deliberately nasty ones) to the running server.

    WEBHOOK_SECRET=s3cret python scripts/send_webhooks.py
Watch the server log and `make worker` output while this runs.
"""
import hashlib
import hmac
import json
import os
import uuid

import httpx

URL = os.getenv("API_URL", "http://127.0.0.1:8000") + "/webhooks/github"
SECRET = os.environ["WEBHOOK_SECRET"]


def sign(body: bytes, secret: str = SECRET) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def send(label: str, event: str, payload: dict | bytes, *, delivery: str | None = None, secret: str = SECRET) -> None:
    body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    headers = {"X-GitHub-Event": event, "X-GitHub-Delivery": delivery or str(uuid.uuid4()),
               "X-Hub-Signature-256": sign(body, secret), "Content-Type": "application/json"}
    r = httpx.post(URL, content=body, headers=headers)
    print(f"{label:<44} -> HTTP {r.status_code} {r.text.strip()}")


def issue(id_: int, state: str, updated_at: str, title: str = "Checkout crashes") -> dict:
    return {"action": "closed" if state == "closed" else "edited",
            "issue": {"id": id_, "number": id_ - 1_000_000, "title": title, "state": state, "updated_at": updated_at}}


send("1. ping", "ping", {"zen": "Keep it logically awesome."})
send("2. issue opened", "issues", issue(1_000_900, "open", "2026-10-01T10:00:00Z"))
d = str(uuid.uuid4())
send("3. issue closed (delivery X)", "issues", issue(1_000_900, "closed", "2026-10-01T11:00:00Z"), delivery=d)
send("4. SAME delivery X again (sender retry)", "issues", issue(1_000_900, "closed", "2026-10-01T11:00:00Z"), delivery=d)
send("5. OLD edit arrives late (out of order)", "issues", issue(1_000_900, "open", "2026-10-01T10:30:00Z", "stale title"))
send("6. wrong secret (attacker)", "issues", issue(1_000_901, "open", "2026-10-01T12:00:00Z"), secret="not-the-secret")
send("7. unsupported event type", "star", {"action": "created"})
send("8. poison: issues event with no 'issue'", "issues", {"action": "opened"})
