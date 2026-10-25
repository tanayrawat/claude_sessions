"""Lesson 8: conditional requests (ETag / 304). Run:  python api-course/08_etag/demo.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx

from common import AUTH, PATH, say, start_server, step, title

title("Lesson 8 - ETag: asking 'has this changed?' almost for free")

server = start_server(n_issues=250, rate_limit=100, window=3600)
URL, PARAMS = server.url + PATH, {"state": "all", "sort": "updated", "direction": "desc", "per_page": 100}

step("1. Poll the same URL 4 times, without and with an ETag. Nothing changes between polls.")
total_plain = 0
for _ in range(4):
    total_plain += len(httpx.get(URL, params=PARAMS, headers=AUTH).content)
print(f"  without ETag: 4 x 200 OK, {total_plain:,} bytes downloaded")

first = httpx.get(URL, params=PARAMS, headers=AUTH)
etag = first.headers["etag"]
total_cond, codes = len(first.content), [first.status_code]
for _ in range(3):
    r = httpx.get(URL, params=PARAMS, headers={**AUTH, "If-None-Match": etag})
    total_cond += len(r.content)
    codes.append(r.status_code)
print(f"  with ETag   : statuses {codes}, {total_cond:,} bytes downloaded")
print(f"  quota left  : {r.headers['x-ratelimit-remaining']} (the three 304s did not consume any)")
say("The ETag is a fingerprint of the response. You send it back in If-None-Match; if it still matches, the server "
    "answers '304 Not Modified' with an empty body. For a pipeline that runs every 5 minutes against a source that "
    "changes twice a day, that is the difference between hammering the API and barely touching it.")

step("2. The trap: an ETag only describes ONE response, i.e. page 1")


def edit_newest_then_poll(direction: str) -> int:
    srv = start_server(n_issues=250)
    params = {"state": "all", "sort": "updated", "direction": direction, "per_page": 100}
    with httpx.Client(base_url=srv.url, headers=AUTH) as c:
        etag = c.get(PATH, params=params).headers["etag"]
        newest = max(srv.issues, key=lambda i: i["updated_at"])  # someone edits the most recently touched issue
        newest["title"], newest["updated_at"] = "EDITED", "2099-01-01T00:00:00Z"
        status = c.get(PATH, params=params, headers={"If-None-Match": etag}).status_code
    srv.stop()
    return status


asc, desc = edit_newest_then_poll("asc"), edit_newest_then_poll("desc")
print(f"  oldest-first : poll after the edit -> {asc}   {'<- WRONG: change is on page 3, page 1 looks unchanged' if asc == 304 else ''}")
print(f"  newest-first : poll after the edit -> {desc}   {'<- correct: new activity always lands on page 1' if desc == 200 else ''}")
say("If you use ETags to decide 'skip this run', sort so that changes always appear on the page whose ETag you check. "
    "That is one more reason the pipeline in this repo asks for newest-first.")
server.stop()
