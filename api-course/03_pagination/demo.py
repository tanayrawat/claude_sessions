"""Lesson 3: pagination, defaults that hide data, and rows that move while you read. Run:  python api-course/03_pagination/demo.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx

from common import AUTH, PATH, say, start_server, step, title

title("Lesson 3 - Pagination: getting ALL the data, correctly")

server = start_server(n_issues=250)
truth = len(server.issues)
print(f"  The source really contains {truth} issues.")

step("1. The naive call: GET the endpoint and use the response")
r = httpx.get(server.url + PATH, headers=AUTH)
rows = r.json()
print(f"  rows received: {len(rows)}   states: {sorted({x['state'] for x in rows})}")
say("Two silent bugs in one call: only the first page (30 rows by default) AND only state=open. "
    "No error, no warning, and the pipeline would report success.")

step("2. Fix: ask for everything explicitly, then follow the Link header until it runs out")
url, params, pages, got = server.url + PATH, {"state": "all", "per_page": 100}, 0, []
while url:
    r = httpx.get(url, params=params, headers=AUTH)
    pages += 1
    got += r.json()
    url, params = r.links.get("next", {}).get("url"), None  # the next URL already contains every parameter
    print(f"  page {pages}: {len(r.json()):>3} rows   Link next -> {'yes' if url else 'none (last page)'}")
print(f"  total {len(got)} / {truth}")
server.stop()


def walk_pages_while_data_changes(direction: str):
    """Page through the data, but a few issues get edited / created right after we read page 1."""
    srv = start_server(n_issues=250)  # same seed, so both runs start from identical data
    before = {i["id"]: i["updated_at"] for i in srv.issues}
    seen = []
    with httpx.Client(base_url=srv.url, headers=AUTH) as c:
        for page in (1, 2, 3):
            resp = c.get(PATH, params={"state": "all", "sort": "updated", "direction": direction,
                                       "per_page": 100, "page": page})
            seen += [i["id"] for i in resp.json()]
            if page == 1:
                srv.mutate()  # the world moves on: 8 issues edited, 4 new ones
    after = {i["id"]: i["updated_at"] for i in srv.issues}
    unchanged = {i for i in before if before[i] == after[i]}
    srv.stop()
    return len(seen) - len(set(seen)), len(unchanged - set(seen))


step("3. Rows move while you read. Does the ORDER you choose matter?")
dup_asc, lost_asc = walk_pages_while_data_changes("asc")
dup_desc, lost_desc = walk_pages_while_data_changes("desc")
print(f"  oldest-first (asc) : duplicates seen = {dup_asc:>2}   UNTOUCHED rows never seen = {lost_asc}")
print(f"  newest-first (desc): duplicates seen = {dup_desc:>2}   UNTOUCHED rows never seen = {lost_desc}")
say("Oldest-first: an edited row jumps to the end, everything behind it slides forward, and a row is stepped over "
    "-> real data loss that nothing will ever recover. Newest-first: edits land on page 1 (already read), "
    "everything slides back, and we just read a few rows twice. Duplicates are harmless if your load is "
    "idempotent (lesson 7). Gaps are not. Prefer duplicates to gaps.")
