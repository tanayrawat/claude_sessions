"""Lesson 9: keep the raw data, validate the rest. Run:  python api-course/09_raw_and_contracts/demo.py"""
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pyarrow.parquet as pq
from pydantic import ValidationError

from common import say, step, title
from ghpipe import bronze
from ghpipe.models import Issue

title("Lesson 9 - Don't trust the payload; don't throw it away either")

GOOD = {"id": 1, "number": 1, "title": "Login broken", "state": "open", "user": {"id": 7, "login": "ana"},
        "labels": [{"name": "bug"}], "created_at": "2026-01-01T10:00:00Z", "updated_at": "2026-01-01T10:00:00Z"}
DRIFT = {**GOOD, "id": 2, "reactions": {"+1": 3}, "type": {"name": "Bug"}}          # provider added fields
NO_TITLE = {**GOOD, "id": 3, "title": None}                                         # required field is null
BAD_ENUM = {**GOOD, "id": 4, "state": "merged"}                                     # value we never agreed to
GHOST = {**GOOD, "id": 5, "user": None}                                             # deleted account
PR = {**GOOD, "id": 6, "pull_request": {"url": "x"}}                                # a PR hiding in the issues list
payloads = [GOOD, DRIFT, NO_TITLE, BAD_ENUM, GHOST, PR]

step("1. A contract (the Issue model) decides what may enter the clean layer")
accepted, rejected = [], []
for p in payloads:
    try:
        accepted.append(Issue.model_validate(p))
    except ValidationError as e:
        err = e.errors()[0]
        rejected.append((p["id"], f"{'.'.join(map(str, err['loc']))}: {err['msg']}"))
print(f"  accepted ids: {[i.id for i in accepted]}")
for id_, why in rejected:
    print(f"  REJECTED id={id_} -> {why}")
by_id = {i.id: i for i in accepted}
print(f"  id=2 had unknown fields (reactions, type): accepted anyway   -> drift must not break the pipeline")
print(f"  id=5 had user=None: accepted (user={by_id[5].user})              -> real data has holes; model them")
print(f"  id=6 is a pull request: is_pull_request = {by_id[6].pull_request is not None}  -> flag it, decide downstream")
say("Rejected rows go to a dead-letter table WITH the reason. They are never silently dropped (numbers go wrong) "
    "and never allowed to crash the whole run (one bad row blocks 9,999 good ones).")

step("2. Why we also store the raw response BEFORE parsing ('bronze')")
tmp = Path(tempfile.mkdtemp())
bronze.write_page(tmp, run_id="run1", source="acme/payments", page_no=1, url="http://x", status=200, records=payloads)
f = bronze.all_files(tmp)[0]
print(f"  stored: {f.relative_to(tmp)}")


def parse_v1(raw):  # yesterday's parser: ignores everything it doesn't know
    d = json.loads(raw)
    return {"id": d["id"], "title": d.get("title")}


def parse_v2(raw):  # today's parser: someone asked for the 'reactions' column
    d = json.loads(raw)
    return {"id": d["id"], "title": d.get("title"), "reactions": d.get("reactions", {}).get("+1", 0)}


raw = pq.read_table(f).column("payload").to_pylist()
print(f"  v1 parser output : {[parse_v1(r) for r in raw][1]}")
print(f"  v2 parser output : {[parse_v2(r) for r in raw][1]}   <- computed from the SAVED data, zero API calls")
say("If we had only stored parsed rows, the new field would exist only for data arriving from today. The API cannot "
    "give us history: it returns the current state of things. Raw storage turns 'we need to re-ingest' into 'we re-run "
    "our code'. The same trick fixes parsing bugs after the fact.")

step("3. The three layers, in one sentence each")
print("  bronze : exactly what the API sent, append-only         (cheap insurance, enables replay)")
print("  silver : validated, typed, de-duplicated, one row per thing   (what engineers query)")
print("  gold   : aggregates shaped for a business question            (what dashboards read)")
