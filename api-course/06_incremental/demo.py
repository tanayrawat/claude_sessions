"""Lesson 6: incremental loading and the watermark. Run:  python api-course/06_incremental/demo.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import PATH, TOKEN, say, start_server, step, title
from ghpipe.client import ApiClient


def fetch(client, since=None):
    params = {"state": "all", "sort": "updated", "direction": "desc", "per_page": 100}
    if since:
        params["since"] = since
    before = client.stats["requests"]
    rows = [r for page in client.paginate(PATH, params) for r in page.json]
    return rows, client.stats["requests"] - before


title("Lesson 6 - Incremental loading: only fetch what changed")
server = start_server(n_issues=250)
client = ApiClient(server.url, TOKEN)

step("1. First run: we have nothing, so we must load everything")
rows, reqs = fetch(client)
warehouse = {r["id"]: r for r in rows}
watermark = max(r["updated_at"] for r in rows)
print(f"  fetched {len(rows)} rows in {reqs} requests;  watermark = {watermark}")
say("The watermark is 'the newest updated_at I have seen'. It is my bookmark: next time, ask only for things changed since then.")

step("2. The source changes: 8 issues edited (3 of them closed) and 4 new ones created")
server.mutate()

step("3a. Naive second run: reload everything")
rows_full, reqs_full = fetch(client)
print(f"  {len(rows_full)} rows, {reqs_full} requests")
step("3b. Incremental second run: ?since=<watermark>")
rows_inc, reqs_inc = fetch(client, since=watermark)
print(f"  {len(rows_inc)} rows, {reqs_inc} request")
say("12 genuinely changed rows (8 edits + 4 new) plus the one boundary row we already had: 'since' is INCLUSIVE "
    "(>=), so we always re-read the newest row we hold. That is deliberate and harmless if loads are idempotent.")
say("At 10 million rows the difference is 100,000 requests vs 1. That is the entire argument for incremental loads.")


def scenario(advance_watermark_first: bool) -> int:
    """Simulate a job that crashes after fetching. Return how many changed rows are still missing afterwards."""
    srv = start_server(n_issues=250)
    c = ApiClient(srv.url, TOKEN)
    rows, _ = fetch(c)
    wh = {r["id"]: r for r in rows}
    state = {"watermark": max(r["updated_at"] for r in rows)}
    srv.mutate()
    changed = {i["id"] for i in srv.issues if i["updated_at"] > state["watermark"]}

    new_rows, _ = fetch(c, since=state["watermark"])  # run N: fetch the changes...
    try:
        if advance_watermark_first:
            state["watermark"] = max(r["updated_at"] for r in new_rows)  # bookmark moved BEFORE the data is safe
        raise RuntimeError("warehouse connection dropped")  # ...and the job crashes while loading
    except RuntimeError:
        pass  # nothing was loaded into `wh`
    retry_rows, _ = fetch(c, since=state["watermark"])  # run N+1 (the scheduler retries)
    wh.update({r["id"]: r for r in retry_rows})
    current = {i["id"]: i["updated_at"] for i in srv.issues}
    lost = sum(1 for id_ in changed if wh.get(id_, {}).get("updated_at") != current[id_])
    srv.stop()
    return lost


step("4. WHEN you move the bookmark matters: the job crashes while loading")
print(f"  bookmark moved BEFORE loading : {scenario(True):>2} changed rows permanently lost")
print(f"  bookmark moved AFTER loading  : {scenario(False):>2} changed rows lost")
say("If the bookmark moves first and the job dies, the next run starts after the lost rows and never sees them. "
    "Rule: load first, verify, and only then advance the watermark. Combined with an idempotent load, a failed run "
    "just means 'read the same window again'.")
server.stop()
