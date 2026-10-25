"""Lesson 1: take an API call apart. Run:  python api-course/01_http/demo.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx

from common import AUTH, PATH, mask, say, start_server, step, title

title("Lesson 1 - What an API call really is")
server = start_server(rate_limit=60, window=60)

step("1. A request = method + URL (+ query parameters) + headers")
req = httpx.Request("GET", server.url + PATH, params={"state": "all", "per_page": 2},
                    headers={**AUTH, "Accept": "application/vnd.github+json"})
print(f"  method : {req.method}")
print(f"  url    : {req.url}")
print(f"  header : Authorization: {mask(req.headers['authorization'])}")
print(f"  header : Accept: {req.headers['accept']}")

step("2. Send it. A response = status code + headers + body")
with httpx.Client() as client:
    r = client.send(req)
print(f"  status : {r.status_code} {r.reason_phrase}")
for h in ("content-type", "etag", "x-ratelimit-limit", "x-ratelimit-remaining"):
    print(f"  header : {h}: {r.headers.get(h)}")
print(f"  header : link: {r.headers.get('link')[:70]}...")

step("3. The body is JSON text. Parsing turns it into Python lists/dicts/None")
data = r.json()
print(f"  top level is a {type(data).__name__} with {len(data)} items (we asked for per_page=2)")
issue = data[0]
print(f"  keys of one item: {sorted(issue)}")
print(f"  nested object   : user = {issue['user']['login']!r} (a dict inside the dict)")
print(f"  a list          : labels = {[l['name'] for l in issue['labels']]}")
print(f"  null -> None    : closed_at = {issue['closed_at']!r}   (open issues have no close time)")

step("4. Status codes tell you WHO is at fault and WHAT to do")
with httpx.Client(base_url=server.url) as client:
    cases = [
        ("valid request", client.get(PATH, headers=AUTH)),
        ("no token", client.get(PATH)),
        ("wrong URL", client.get("/repos/acme/payments/nope", headers=AUTH)),
    ]
for label, resp in cases:
    print(f"  {label:<14} -> {resp.status_code}  {resp.text[:50]!r}")

step("5. There is a SECOND kind of failure: no HTTP response at all")
try:
    httpx.get("http://127.0.0.1:9/", timeout=2)  # nothing listens on port 9
except httpx.TransportError as e:
    print(f"  raised {type(e).__name__}: this is an exception, not a status code")
say("Real pipelines must handle both kinds: bad status codes AND network exceptions (timeouts, resets, DNS).")

server.stop()
