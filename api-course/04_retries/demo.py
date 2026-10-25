"""Lesson 4: failures, retries, backoff and jitter. Run:  python api-course/04_retries/demo.py"""
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx

from common import AUTH, PATH, TOKEN, say, start_server, step, title
from ghpipe.client import RETRYABLE_STATUS, ApiClient

title("Lesson 4 - When calls fail: retry the right things, the right way")
server = start_server()
URL = server.url + PATH
PARAMS = {"state": "all", "per_page": 100}

step("1. Same bad luck for both callers: the next three requests fail (500, 503, 429)")
server.faults = [500, 503, 429]
try:
    httpx.get(URL, params=PARAMS, headers=AUTH).raise_for_status()
except httpx.HTTPStatusError as e:
    print(f"  naive caller  : crashed on the first blip -> {e.response.status_code}")
    print("                  (in production: a 3am page for a problem that fixed itself 1 second later)")

server.faults = [500, 503, 429]
waits = []
client = ApiClient(server.url, TOKEN, sleep=waits.append, rng=random.Random(4))  # sleep is recorded, not slept
rows = client.get(PATH, PARAMS).json
print(f"  robust caller : succeeded, got {len(rows)} rows after {client.stats['retries']} retries")
print(f"                  waits chosen: {[round(w, 2) for w in waits]} seconds")
say("The 3rd wait is exactly 1.0 because the server's 429 response said 'Retry-After: 1'. "
    "When the server tells you how long to wait, believe it over your own guess.")

step("2. Which responses deserve a retry?")
for code in (200, 400, 401, 403, 404, 408, 422, 429, 500, 502, 503):
    verdict = "retry" if code in RETRYABLE_STATUS else ("success" if code < 300 else "STOP - fix the request/config")
    print(f"  {code}  {verdict}")
say("Rule of thumb: 5xx and 429 mean 'the server is struggling, try later'. 4xx mean 'YOU sent something wrong' "
    "and the same request will fail identically forever. (403 is special: it is retryable only when it means 'quota exhausted'.)")

step("3. Why 'jitter'? 2000 clients all fail at the same instant")
rng = random.Random(0)
N = 2000


def histogram(times, bucket=0.1, width=40):
    counts = {}
    for t in times:
        counts[int(t / bucket)] = counts.get(int(t / bucket), 0) + 1
    peak = max(counts.values())
    for b in range(0, 11):
        n = counts.get(b, 0)
        print(f"    t={b * bucket:0.1f}s  {'#' * round(n / peak * width):<{width}} {n}")
    return peak


print("  (a) every client waits exactly 1.0s, then retries:")
peak_fixed = histogram([1.0] * N)
print("  (b) every client waits a random time between 0 and 1.0s ('full jitter'):")
peak_jitter = histogram([rng.uniform(0, 1.0) for _ in range(N)])
print(f"  Busiest 0.1s for the struggling server: {peak_fixed} requests vs {peak_jitter}.")
say("Without jitter the retries arrive as a second wave exactly when the server is trying to recover - a 'retry storm'. "
    "Randomness spreads the load. This is why every serious client uses exponential backoff WITH jitter.")

step("4. And always give up eventually")
server.faults = [503] * 20
try:
    ApiClient(server.url, TOKEN, sleep=lambda s: None, max_retries=3).get(PATH, PARAMS)
except Exception as e:
    print(f"  after 3 retries: {type(e).__name__}")
say("Raise a clear error so the orchestrator marks the task failed and a human or the scheduler's own retry takes over.")
server.stop()
