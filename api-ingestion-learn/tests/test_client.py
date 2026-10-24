import pytest

from ghpipe.client import ApiClient, ApiError, RateLimitExceeded, RetriesExhausted
from ghpipe.mock_server import MockGitHub

PATH = "/repos/acme/payments/issues"
ALL = {"state": "all", "per_page": 100}


def test_state_defaults_to_open_only(make_client):
    # the classic silent-data-loss bug: forgetting state=all
    c = make_client()
    open_only = [r for p in c.paginate(PATH, {"per_page": 100}) for r in p.json]
    everything = [r for p in c.paginate(PATH, ALL) for r in p.json]
    assert 0 < len(open_only) < len(everything) == 250


def test_pagination_follows_link_header(make_client):
    pages = list(make_client().paginate(PATH, ALL))
    assert [len(p.json) for p in pages] == [100, 100, 50]
    ids = [r["id"] for p in pages for r in p.json]
    assert len(ids) == len(set(ids)) == 250


def test_retries_5xx_then_succeeds(make_client, server, waits):
    server.faults = [500, 502]
    c = make_client()
    assert len(c.get(PATH, ALL).json) == 100
    assert c.stats["retries"] == 2 and len(waits) == 2


def test_backoff_is_jittered_and_capped(make_client, server, waits):
    server.faults = [500] * 5
    make_client(max_retries=5, backoff_base=1.0, backoff_cap=4.0).get(PATH, ALL)
    assert all(0 <= w <= min(4.0, 2**i) for i, w in enumerate(waits))


def test_honours_retry_after_on_429(make_client, server, waits):
    server.faults = [429]
    make_client().get(PATH, ALL)
    assert waits == [1.0]  # server said Retry-After: 1; we do not substitute our own guess


def test_gives_up_after_max_retries(make_client, server):
    server.faults = [503] * 10
    with pytest.raises(RetriesExhausted):
        make_client(max_retries=2).get(PATH, ALL)


def test_does_not_retry_client_errors(server, waits):
    c = ApiClient(server.url, "wrong-token", sleep=waits.append)
    with pytest.raises(ApiError) as e:
        c.get(PATH, ALL)
    assert e.value.status == 401 and c.stats["retries"] == 0 and waits == []

    with pytest.raises(ApiError) as e:
        ApiClient(server.url, "dev-token", sleep=waits.append).get("/nope")
    assert e.value.status == 404


def test_etag_conditional_request_returns_304(make_client):
    c = make_client()
    first = c.get(PATH, ALL)
    again = c.get(PATH, ALL, etag=first.etag)
    assert again.not_modified and again.json is None


def test_proactively_waits_for_quota_reset():
    srv = MockGitHub(n_issues=250, rate_limit=3, window=1).start()  # 3 pages needed; client stops at 1 remaining
    try:
        c = ApiClient(srv.url, "dev-token")  # real sleep: ~2s
        rows = [r for p in c.paginate(PATH, ALL) for r in p.json]
        assert len(rows) == 250 and c.stats["throttle_sleeps"] == 1 and c.stats["retries"] == 0
    finally:
        srv.stop()


def test_refuses_to_sleep_for_an_hour(waits):
    srv = MockGitHub(n_issues=250, rate_limit=1, window=3600).start()
    try:
        c = ApiClient(srv.url, "dev-token", sleep=waits.append, max_wait=5)
        with pytest.raises(RateLimitExceeded) as e:
            list(c.paginate(PATH, ALL))
        assert e.value.wait_seconds > 3000 and waits == []  # fail fast, let the orchestrator reschedule
    finally:
        srv.stop()
