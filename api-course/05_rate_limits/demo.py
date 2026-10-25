"""Lesson 5: rate limits. Run:  python api-course/05_rate_limits/demo.py   (takes ~15 seconds: it really waits)"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx

from common import AUTH, PATH, TOKEN, say, start_server, step, title
from ghpipe.client import ApiClient, RateLimitExceeded

title("Lesson 5 - Rate limits: a budget of requests per time window")

step("1. The budget is visible in the response headers")
server = start_server(rate_limit=5, window=3)
for i in range(1, 8):
    r = httpx.get(server.url + PATH, params={"per_page": 1}, headers=AUTH)
    print(f"  request {i}: status={r.status_code}  remaining={r.headers['x-ratelimit-remaining']}"
          f"  limit={r.headers['x-ratelimit-limit']}")
say("Requests 6 and 7 got 403, not the 429 you might expect. GitHub reports an exhausted quota as 403 with "
    "remaining=0. The status code alone is not enough: you must read the headers.")
server.stop()

step("2. A polite client reads the budget and slows down BEFORE hitting zero")
server = start_server(n_issues=250, rate_limit=5, window=3)
client = ApiClient(server.url, TOKEN)  # real sleeping this time
start = time.time()
rows = [r for page in client.paginate(PATH, {"state": "all", "per_page": 30}) for r in page.json]
print(f"  fetched {len(rows)} rows in {time.time() - start:.1f}s using {client.stats['requests']} requests")
print(f"  paused {client.stats['throttle_sleeps']} time(s) to wait for the quota to reset; errors: {client.stats['retries']}")
server.stop()

step("3. What if the wait is an hour? Don't sleep inside a task - fail fast")
server = start_server(n_issues=250, rate_limit=1, window=3600)
client = ApiClient(server.url, TOKEN, max_wait=5)
try:
    list(client.paginate(PATH, {"state": "all", "per_page": 30}))
except RateLimitExceeded as e:
    print(f"  raised RateLimitExceeded: would need to wait {e.wait_seconds:.0f}s (max_wait is 5s)")
say("A worker asleep for an hour is a wasted slot that hides the problem. Failing fast lets the orchestrator "
    "reschedule the task for after the reset, and lets monitoring notice if it happens every day.")
server.stop()
