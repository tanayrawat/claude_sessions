import dataclasses

import duckdb
from fastapi.testclient import TestClient

from app.main import create_app
from .conftest import KEY


def walk(client, **params):
    """Follow next_cursor to the end, returning every row in the order served."""
    rows, cursor = [], None
    while True:
        r = client.get("/v1/issues", headers=KEY, params={**params, **({"cursor": cursor} if cursor else {})})
        assert r.status_code == 200
        body = r.json()
        rows += body["data"]
        cursor = body["next_cursor"]
        if not cursor:
            return rows


# ---- stage 1: reading ----------------------------------------------------------------------------------------
def test_health_needs_no_key_but_data_does(client):
    assert client.get("/health").status_code == 200
    assert client.get("/v1/issues").status_code == 401
    assert client.get("/v1/issues", headers={"X-API-Key": "nope"}).status_code == 401


def test_prs_excluded_by_default_and_filters_work(client):
    issues = walk(client, limit=25)
    assert issues and not any(i["is_pull_request"] for i in issues)
    assert any(i["is_pull_request"] for i in walk(client, include_prs=True, limit=25))
    assert {i["state"] for i in walk(client, state="closed")} == {"closed"}
    assert all("bug" in i["labels"] for i in walk(client, label="bug"))


def test_cursor_pagination_has_no_gaps_or_duplicates_even_with_tied_timestamps(client):
    rows = walk(client, include_prs=True, limit=7)
    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids)) == 120  # every row exactly once
    keys = [(r["updated_at"], r["id"]) for r in rows]
    assert keys == sorted(keys, reverse=True)
    assert len({r["updated_at"] for r in rows}) < 120  # the seed really does contain ties


def test_keyset_is_stable_when_new_rows_arrive_mid_pagination(client, settings):
    """The reason cursors beat OFFSET: a new top row must not shift the pages we haven't read yet."""
    first = client.get("/v1/issues", headers=KEY, params={"limit": 10, "include_prs": True}).json()
    con = duckdb.connect(str(settings.db_path))
    con.execute("""INSERT INTO silver.issues VALUES (9999999, 'acme/payments', 999, 'brand new', 'open', 1, 'u', [], 0,
                   false, TIMESTAMP '2030-01-01', TIMESTAMP '2030-01-01', NULL)""")
    con.close()
    second = client.get("/v1/issues", headers=KEY,
                        params={"limit": 10, "include_prs": True, "cursor": first["next_cursor"]}).json()
    assert not {r["id"] for r in first["data"]} & {r["id"] for r in second["data"]}  # OFFSET would repeat 1 row here
    assert 9999999 not in {r["id"] for r in second["data"]}


def test_bad_inputs_are_clean_4xx_not_500(client):
    assert client.get("/v1/issues", headers=KEY, params={"cursor": "garbage!!"}).status_code == 400
    assert client.get("/v1/issues", headers=KEY, params={"limit": 100000}).status_code == 422
    assert client.get("/v1/issues", headers=KEY, params={"state": "x'; DROP TABLE silver.issues;--"}).status_code == 422
    assert client.get("/v1/issues", headers=KEY, params={"label": "x'; DROP TABLE silver.issues;--"}).json()["data"] == []
    assert client.get("/v1/issues/424242", headers=KEY).status_code == 404
    assert client.get("/v1/issues/1000001", headers=KEY).status_code == 200


def test_ready_reports_503_when_warehouse_missing(settings):
    broken = dataclasses.replace(settings, db_path=settings.db_path.parent / "missing.duckdb")
    assert TestClient(create_app(broken)).get("/ready").status_code == 503


# ---- stage 2: protecting it ----------------------------------------------------------------------------------
def test_etag_returns_304_until_data_changes(client, settings):
    r1 = client.get("/v1/issues", headers=KEY)
    assert r1.headers["etag"]
    r2 = client.get("/v1/issues", headers={**KEY, "If-None-Match": r1.headers["etag"]})
    assert r2.status_code == 304 and r2.content == b""
    con = duckdb.connect(str(settings.db_path))
    con.execute("UPDATE silver.issues SET title = 'changed' WHERE id = (SELECT id FROM silver.issues WHERE NOT is_pull_request ORDER BY updated_at DESC, id DESC LIMIT 1)")
    con.close()
    r3 = client.get("/v1/issues", headers={**KEY, "If-None-Match": r1.headers["etag"]})
    assert r3.status_code == 200


def test_rate_limit_is_per_client_and_recovers(settings, clock):
    c = TestClient(create_app(dataclasses.replace(settings, rate_capacity=3, rate_refill_per_sec=1.0), clock))
    codes = [c.get("/v1/issues", headers=KEY).status_code for _ in range(4)]
    assert codes == [200, 200, 200, 429]
    blocked = c.get("/v1/issues", headers=KEY)
    assert blocked.status_code == 429 and blocked.headers["retry-after"] == "1" and blocked.headers["x-ratelimit-remaining"] == "0"
    assert c.get("/v1/issues", headers={"X-API-Key": "k-bi"}).status_code == 200  # other client unaffected
    clock.t += 1.0  # one token refills per second
    ok = c.get("/v1/issues", headers=KEY)
    assert ok.status_code == 200 and ok.headers["x-ratelimit-limit"] == "3"
