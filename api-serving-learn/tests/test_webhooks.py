import hashlib
import hmac
import json

from app import inbox
from app.main import verify_signature
from .conftest import KEY

SECRET = "s3cret"


def post(client, payload, *, delivery="d1", event="issues", secret=SECRET, raw: bytes | None = None, sig: str | None = None):
    body = raw if raw is not None else json.dumps(payload).encode()
    signature = sig if sig is not None else "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return client.post("/webhooks/github", content=body, headers={
        "X-GitHub-Delivery": delivery, "X-GitHub-Event": event, "X-Hub-Signature-256": signature})


def issue(id_=1, state="open", updated="2026-10-01T10:00:00Z", title="t"):
    return {"issue": {"id": id_, "number": id_, "title": title, "state": state, "updated_at": updated}}


def run_worker(settings):
    conn = inbox.connect(settings.inbox_path)
    try:
        return inbox.process_pending(conn, settings.max_attempts), conn.execute("SELECT * FROM issue_state").fetchall()
    finally:
        conn.close()


def count(settings, where="1=1"):
    conn = inbox.connect(settings.inbox_path)
    try:
        return conn.execute(f"SELECT count(*) FROM inbox WHERE {where}").fetchone()[0]
    finally:
        conn.close()


def test_rejects_bad_or_missing_signature_and_stores_nothing(client, settings):
    assert post(client, issue(), secret="wrong").status_code == 401
    assert post(client, issue(), sig="").status_code == 401
    assert post(client, issue(), sig="sha256=" + "0" * 64).status_code == 401
    assert count(settings) == 0


def test_signature_is_over_raw_bytes_not_reserialised_json(client):
    pretty = json.dumps(issue(), indent=2).encode()
    sig = "sha256=" + hmac.new(SECRET.encode(), pretty, hashlib.sha256).hexdigest()
    assert post(client, None, raw=pretty, sig=sig).status_code == 202  # valid over the bytes actually sent
    compact = json.dumps(issue()).encode()  # same JSON *value*, different bytes
    assert post(client, None, raw=compact, sig=sig, delivery="d2").status_code == 401
    assert verify_signature(SECRET, b"x", None) is False


def test_fails_closed_without_a_configured_secret(settings, clock):
    import dataclasses
    from fastapi.testclient import TestClient
    from app.main import create_app
    c = TestClient(create_app(dataclasses.replace(settings, webhook_secret=None), clock))
    assert post(c, issue()).status_code == 503


def test_ack_is_fast_and_duplicates_are_idempotent(client, settings):
    assert post(client, issue()).status_code == 202
    again = post(client, issue())  # sender retried the same delivery
    assert again.status_code == 200 and again.json() == {"status": "duplicate"}
    assert count(settings) == 1
    assert count(settings, "status='pending'") == 1  # nothing was processed inside the request


def test_malformed_requests_get_400(client):
    assert post(client, None, raw=b"not json").status_code == 400
    body = json.dumps(issue()).encode()
    sig = "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    assert client.post("/webhooks/github", content=body, headers={"X-Hub-Signature-256": sig}).status_code == 400


def test_out_of_order_delivery_cannot_overwrite_newer_state(client, settings):
    post(client, issue(state="closed", updated="2026-10-01T11:00:00Z", title="final"), delivery="new")
    post(client, issue(state="open", updated="2026-10-01T10:00:00Z", title="stale"), delivery="old")  # arrives later
    stats, state = run_worker(settings)
    assert stats["processed"] == 2
    assert [(r["title"], r["state"]) for r in state] == [("final", "closed")]


def test_unknown_events_ignored_poison_parked_ping_ok(client, settings):
    post(client, {"zen": "x"}, event="ping", delivery="a")
    post(client, {"action": "created"}, event="star", delivery="b")
    post(client, {"action": "opened"}, event="issues", delivery="c")  # no 'issue' object: can never succeed
    stats, _ = run_worker(settings)
    assert (stats["processed"], stats["ignored"], stats["dead"]) == (1, 1, 1)
    assert run_worker(settings)[0] == {}  # nothing left; dead letters are not retried forever
    assert client.get("/v1/admin/inbox", headers=KEY).json()["by_status"] == {"processed": 1, "ignored": 1, "dead": 1}


def test_transient_failures_retry_then_park(client, settings, monkeypatch):
    post(client, issue())
    calls = {"n": 0}

    def flaky(*a, **k):
        calls["n"] += 1
        raise RuntimeError("downstream down")

    monkeypatch.setattr(inbox, "_apply", flaky)
    assert run_worker(settings)[0]["failed"] == 1
    assert run_worker(settings)[0]["failed"] == 1
    assert run_worker(settings)[0]["dead"] == 1  # 3rd attempt hits max_attempts
    assert run_worker(settings)[0] == {} and calls["n"] == 3


def test_a_failed_message_succeeds_after_the_outage_ends(client, settings, monkeypatch):
    post(client, issue())
    real = inbox._apply
    monkeypatch.setattr(inbox, "_apply", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("blip")))
    assert run_worker(settings)[0]["failed"] == 1
    monkeypatch.setattr(inbox, "_apply", real)  # outage over
    stats, state = run_worker(settings)
    assert stats["processed"] == 1 and len(state) == 1
