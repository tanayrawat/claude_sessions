import dataclasses

import pyarrow.parquet as pq
import pytest

from ghpipe import bronze, warehouse
from ghpipe.client import ApiClient
from ghpipe.pipeline import run_pipeline
from ghpipe.quality import DataQualityError


@pytest.fixture
def client(make_client):
    return make_client()


def q(settings, sql):
    con = warehouse.connect(settings.db_path)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_first_run_is_a_full_load(settings, client):
    r = run_pipeline(settings, client)
    assert (r.status, r.fetched, r.inserted, r.rejected) == ("success", 250, 250, 0)
    assert r.requests == 3  # 250 rows / 100 per page
    assert len(bronze.all_files(settings.bronze_dir)) == 3
    n, prs = q(settings, "SELECT count(*), count(*) FILTER (WHERE is_pull_request) FROM silver.issues")[0]
    assert n == 250 and 0 < prs < 250  # PRs are kept but flagged; gold excludes them
    assert q(settings, "SELECT count(*) FROM gold.daily_issue_activity")[0][0] > 0


def test_polling_with_no_changes_becomes_a_304(settings, client):
    run_pipeline(settings, client)
    run_pipeline(settings, client)  # first incremental URL (has since=) differs from the full-load URL: no ETag to send yet
    r = run_pipeline(settings, client)
    assert (r.status, r.fetched, r.requests) == ("no_change", 0, 1)
    assert q(settings, "SELECT count(*) FROM silver.issues")[0][0] == 250


def test_incremental_run_only_pulls_changes(settings, client, server):
    run_pipeline(settings, client)
    server.mutate()
    r = run_pipeline(settings, client)
    assert r.status == "success" and 0 < r.fetched < 40
    assert (r.inserted, r.updated) == (4, 8)
    assert q(settings, "SELECT count(*) FROM silver.issues")[0][0] == 254
    assert r.watermark == max(i["updated_at"] for i in server.issues)


def test_etag_does_not_hide_changes_on_later_pages(settings, make_client, server):
    """Regression for a real trap: page-1 ETag unchanged while new data lands on page N. desc ordering avoids it."""
    s = dataclasses.replace(settings, per_page=3)
    client = make_client()
    run_pipeline(s, client)
    server.mutate()
    r = run_pipeline(s, client)
    assert r.status == "success" and (r.inserted, r.updated) == (4, 8)


def test_pipeline_is_idempotent(settings, client):
    run_pipeline(settings, client)
    before = q(settings, "SELECT id, updated_at, title FROM silver.issues ORDER BY id")
    con = warehouse.connect(settings.db_path)
    con.execute("DELETE FROM ops.pipeline_state")  # force a full re-read of everything
    con.close()
    r = run_pipeline(settings, client)
    assert r.fetched == 250 and r.inserted == 0
    assert q(settings, "SELECT id, updated_at, title FROM silver.issues ORDER BY id") == before


def test_stale_data_never_overwrites_newer(settings, client, server):
    run_pipeline(settings, client)
    con = warehouse.connect(settings.db_path)
    newest = con.execute("SELECT max(updated_at) FROM silver.issues").fetchone()[0]
    files = bronze.all_files(settings.bronze_dir)
    con.execute("UPDATE silver.issues SET title = 'EDITED LATER', updated_at = ? WHERE id = 1000001",
                [newest.replace(year=newest.year + 1)])
    warehouse.load_silver(con, files)  # replaying older bronze over it
    assert con.execute("SELECT title FROM silver.issues WHERE id = 1000001").fetchone()[0] == "EDITED LATER"
    con.close()


def test_bad_records_are_quarantined_and_drift_is_tolerated(settings, make_client, server):
    s = dataclasses.replace(settings, max_reject_rate=0.5)
    c = make_client()
    run_pipeline(s, c)
    server.mutate(bad=True)
    r = run_pipeline(s, c)
    assert r.status == "success" and r.rejected == 2
    dl = q(s, "SELECT error FROM silver.dead_letter ORDER BY error")
    assert len(dl) == 2 and any("title" in e[0] for e in dl) and any("state" in e[0] for e in dl)
    # the drifted record (extra fields) loads fine and the raw payload keeps what silver ignores
    assert q(s, "SELECT count(*) FROM silver.issues WHERE number = 257")[0][0] == 1
    raw = [p for f in bronze.all_files(s.bronze_dir) for p in pq.read_table(f).column("payload").to_pylist()]
    assert any('"reactions"' in p for p in raw)


def test_dq_failure_blocks_watermark_and_recovers(settings, client, server):
    run_pipeline(settings, client)
    wm = q(settings, "SELECT watermark FROM ops.pipeline_state")[0][0]
    server.mutate(bad=True)  # 2 of ~15 fetched rows rejected -> above the default 5% gate
    with pytest.raises(DataQualityError, match="reject_rate"):
        run_pipeline(settings, client)
    assert q(settings, "SELECT watermark FROM ops.pipeline_state")[0][0] == wm  # did not advance
    assert q(settings, "SELECT status FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 1")[0][0] == "failed"

    r = run_pipeline(dataclasses.replace(settings, max_reject_rate=0.5), client)  # operator relaxes the gate
    assert r.status == "success" and r.watermark > wm


def test_survives_transient_api_failures(settings, client, server, waits):
    server.faults = [500, 429, 502]
    r = run_pipeline(settings, client)
    assert r.status == "success" and r.retries == 3 and r.inserted == 250


def test_replay_rebuilds_silver_from_bronze_without_the_api(settings, client, server):
    run_pipeline(settings, client)
    server.mutate()
    run_pipeline(settings, client)
    expected = q(settings, "SELECT * FROM silver.issues ORDER BY id")
    server.stop()  # prove no API calls are needed
    con = warehouse.connect(settings.db_path)
    warehouse.rebuild_silver(con, bronze.all_files(settings.bronze_dir))
    con.close()
    rebuilt = q(settings, "SELECT * FROM silver.issues ORDER BY id")
    strip = lambda rows: [r[:-2] for r in rows]  # ignore _run_id/_loaded_at lineage columns
    assert strip(rebuilt) == strip(expected)
    server.start = None  # fixture teardown calls stop() again; make it harmless
