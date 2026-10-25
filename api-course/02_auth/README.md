# Lesson 2: Authentication and secrets

**Time:** 30 min · **Needs:** Lesson 1 · **Run:** `python api-course/02_auth/demo.py`

## The problem

An API that gave data to anyone would leak customer data and could be abused. So providers need to know *who is asking*
(to allow them, to rate-limit them, to bill them). You must send a credential with every request. And now you have a new
risk: a credential in your code or logs is a security incident waiting to happen. Leaked tokens are among the most common real-world
causes of data breaches, and the pipeline engineer is often the person who handles the token.

## The idea

**Authentication** = proving who you are. **Authorization** = what you're allowed to do once known. The two
status codes map to these:

| Status | Meaning | Is retrying useful? |
|---|---|---|
| `401 Unauthorized` | "I don't know who you are" (missing, wrong or expired credential) | No: fix or refresh the credential |
| `403 Forbidden` | "I know who you are, and you may not do this" (insufficient permission) | No: ask for access. (Careful: some APIs also use 403 for an exhausted quota, lesson 5) |

(The naming is historically confusing: `401` is really about *authentication*.)

### The common ways APIs authenticate you

| Scheme | How it looks | Seen in |
|---|---|---|
| **API key** | header `X-API-Key: abc` or `?api_key=abc` | Many simple SaaS APIs |
| **Bearer token** | header `Authorization: Bearer <token>` | GitHub personal access tokens, most modern APIs |
| **OAuth 2.0** | You exchange a client id + secret (or a user login) for a short-lived access token, plus a *refresh token* to get new ones | Google, Salesforce, Slack, HubSpot |
| **Basic auth** | `Authorization: Basic base64(user:pass)` | Older systems, some internal ones |

OAuth is the one that adds real engineering work: **access tokens expire** (often in an hour). A pipeline that runs
at 3am must detect a `401`, use the refresh token to get a new access token, and retry once, otherwise it breaks the moment a token expires
and nobody notices until the data is stale. Never put `?api_key=` in query strings if you can avoid it: URLs get logged by proxies and servers.

## How secrets should be handled

1. **Never** put a secret in code or in git. It stays in history even if you delete it later.
2. Read it from the **environment** (`os.environ["GITHUB_TOKEN"]`), which your orchestrator or deployment fills from a **secrets manager**
   (AWS Secrets Manager, Vault, Airflow Connections, GitHub Actions secrets).
3. Fail **loudly** if it is missing: `os.environ["X"]` raises `KeyError`; `os.getenv("X")` quietly returns `None` and you
   make unauthenticated calls with a much lower rate limit, which looks like a mysterious slowdown.
4. **Redact** before logging. Logs, stack traces and CI output are where tokens actually leak.
5. Give each pipeline its own least-privilege token (read-only, one repo) so a leak has a small blast radius, and rotate on a schedule.

## Run it

**Predict:** which of the three callers gets a `200`? What does a naive log line show?

```
  no token      -> 401
  wrong token   -> 401
  correct token -> 200
...
  naive log line   : Bearer dev-token       <- the secret, in your logs
  redacted log line: ***
```

## In real jobs

Auth failures are a classic 3am page: a token expired, was rotated, or its owner left the company and the account was disabled.
Good pipelines distinguish them from transient errors: `401/403` should **fail the run immediately with a clear alert**, not retry for an hour.
GitHub also returns `404` (not `403`) for private repos you can't access, to avoid revealing they exist; a "not found" can really mean "no permission".

## Try it yourself

1. In `demo.py`, send the token as `Token ...` instead of `Bearer ...`. What happens and why?
2. Write a `redact()` that also hides tokens in `?api_key=` URL query strings.
3. Search your own past projects for a hard-coded secret. (Seriously. Rotate it if you find one.)

## Check yourself

1. Your daily job starts returning `401` after working for weeks. Name two likely causes.
2. Why is `os.environ["TOKEN"]` better than `os.getenv("TOKEN")` for a required secret?
3. Should the pipeline retry a `401` five times with backoff?

<details><summary>Answers</summary>

1. The token expired (OAuth access tokens do) or was revoked/rotated, or the account/owner was disabled.
2. A missing secret raises immediately with a clear error. `getenv` returns `None`, and you may silently send unauthenticated requests with lower limits or confusing errors.
3. No. The credential won't fix itself. Refresh it once if the scheme supports that (OAuth), otherwise fail fast and alert.
</details>

**Next:** [Lesson 3: Pagination](../03_pagination/README.md)
