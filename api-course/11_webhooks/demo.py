"""Lesson 11: webhooks (push instead of pull). Run:  python api-course/11_webhooks/demo.py"""
import hashlib
import hmac
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import say, step, title, use_serving_project

use_serving_project()
from fastapi.testclient import TestClient

from app import inbox
from app.main import create_app
from app.settings import Settings, hash_key

SECRET = "s3cret"
tmp = Path(tempfile.mkdtemp())
settings = Settings(db_path=tmp / "wh.duckdb", inbox_path=tmp / "inbox.sqlite", webhook_secret=SECRET,
                    api_key_hashes={hash_key("k"): "demo"})
client = TestClient(create_app(settings))

title("Lesson 11 - Webhooks: the provider calls YOU")


def deliver(label, event, payload, delivery, secret=SECRET, body=None):
    body = body if body is not None else json.dumps(payload).encode()
    sig = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    r = client.post("/webhooks/github", content=body, headers={
        "X-GitHub-Event": event, "X-GitHub-Delivery": delivery, "X-Hub-Signature-256": sig})
    print(f"  {label:<46} -> HTTP {r.status_code}  {r.json()}")


def issue(state, updated, title_="Checkout crashes"):
    return {"action": "edited", "issue": {"id": 42, "number": 42, "title": title_, "state": state, "updated_at": updated}}


step("1. Deliveries arrive. Watch the HTTP answers: the sender decides whether to retry based on them")
deliver("issue closed at 11:00", "issues", issue("closed", "2026-10-01T11:00:00Z", "final title"), "delivery-A")
deliver("SAME delivery again (the sender retried)", "issues", issue("closed", "2026-10-01T11:00:00Z", "final title"), "delivery-A")
deliver("older edit from 10:00 arrives late", "issues", issue("open", "2026-10-01T10:00:00Z", "stale title"), "delivery-B")
deliver("forged request (wrong secret)", "issues", issue("open", "2026-10-01T12:00:00Z"), "delivery-C", secret="attacker")
deliver("event type we don't handle", "star", {"action": "created"}, "delivery-D")
deliver("broken payload (no 'issue' inside)", "issues", {"action": "opened"}, "delivery-E")
say("The duplicate got 200 'duplicate', not an error: from the sender's point of view it succeeded, so it stops retrying. "
    "The forgery got 401 and nothing was stored. Everything else was just written to the inbox and acknowledged immediately.")

step("2. Nothing has been processed yet - the web request only stored the raw events")
conn = inbox.connect(settings.inbox_path)
print("  inbox status counts:", inbox.stats(conn)["by_status"])

step("3. A separate worker drains the inbox")
print("  worker result:", dict(inbox.process_pending(conn)))
print("  inbox status counts:", inbox.stats(conn)["by_status"])
row = conn.execute("SELECT state, title, updated_at FROM issue_state WHERE id = 42").fetchone()
print(f"  issue 42 now: state={row['state']!r} title={row['title']!r} updated_at={row['updated_at']}")
say("The late-arriving 10:00 edit did not overwrite the 11:00 version: the same 'only newer or equal may win' guard from "
    "lesson 7. The broken payload was parked as 'dead' immediately (retrying can never fix it); the unknown event type "
    "was 'ignored' (a valid event we chose not to handle is not an error).")

step("4. The classic signature bug: verifying the wrong bytes")
pretty = json.dumps(issue("open", "2026-10-02T00:00:00Z"), indent=2).encode()
sig_over_pretty = "sha256=" + hmac.new(SECRET.encode(), pretty, hashlib.sha256).hexdigest()
reserialized = json.dumps(json.loads(pretty)).encode()  # what you'd verify if you parsed first and re-serialised
print("  signature matches the bytes actually sent  :", hmac.compare_digest(sig_over_pretty, "sha256=" + hmac.new(SECRET.encode(), pretty, hashlib.sha256).hexdigest()))
print("  signature matches the re-serialised JSON   :", hmac.compare_digest(sig_over_pretty, "sha256=" + hmac.new(SECRET.encode(), reserialized, hashlib.sha256).hexdigest()))
say("Same data, different whitespace, different bytes, different HMAC. Always verify the raw request body, before any parsing.")
conn.close()
