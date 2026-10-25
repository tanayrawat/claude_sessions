"""Lesson 2: authentication and secrets. Run:  python api-course/02_auth/demo.py"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx

from common import PATH, TOKEN, say, start_server, step, title

title("Lesson 2 - Proving who you are, without leaking it")
server = start_server()


def call(token):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.get(server.url + PATH, headers=headers).status_code


step("1. Three callers, three outcomes")
print(f"  no token      -> {call(None)}")
print(f"  wrong token   -> {call('wrong-token')}")
print(f"  correct token -> {call(TOKEN)}")
say("401 means 'I don't know who you are'. A 403 would mean 'I know who you are, but you may not do this'. "
    "Neither can be fixed by retrying: they are YOUR configuration problem, so a pipeline should stop and alert.")

step("2. Where the token comes from: the environment, never the code")
os.environ["GITHUB_TOKEN"] = TOKEN  # in real life your shell / Airflow connection / secrets manager sets this
token = os.environ["GITHUB_TOKEN"]  # KeyError (loud failure) is better than silently running unauthenticated
print("  token = os.environ['GITHUB_TOKEN']      # good: secret lives outside the repo")
print("  token = 'ghp_abc123...'                 # BAD: now it is in git history forever")

step("3. The quiet way tokens leak: logging")
req = httpx.Request("GET", server.url + PATH, headers={"Authorization": f"Bearer {TOKEN}"})
print("  naive log line   :", dict(req.headers)["authorization"])


def redact(headers):
    return {k: ("***" if k.lower() in {"authorization", "x-api-key", "cookie"} else v) for k, v in headers.items()}


print("  redacted log line:", redact(req.headers)["authorization"])
say("Debug logs, exception reports and CI output are the usual places a token escapes. Redact before logging.")

server.stop()
