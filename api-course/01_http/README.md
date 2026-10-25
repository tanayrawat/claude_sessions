# Lesson 1: What an API call really is

**Time:** 45 min · **Needs:** nothing · **Run:** `python api-course/01_http/demo.py`

## The problem

Your company wants a dashboard of customer support tickets. The tickets live in a SaaS tool (Zendesk, Jira, GitHub...).
You can't connect to their database and run SQL. What you *can* do is ask their servers questions over the internet,
in a format they define. That format is the **API** (Application Programming Interface), and in data engineering,
**most data that doesn't originate in your own databases arrives through one.**

## The idea

Think of a restaurant. The **menu** lists what you may order (the API's *endpoints*). You write an **order**
(the *request*). The **waiter** carries it over (the *HTTP protocol*). The **kitchen** (the *server*) prepares the answer and
the waiter brings back a **dish, or a reason it can't be made** (the *response* and its *status code*).

An API call is nothing more mysterious than that exchange of one message each way.

## The anatomy

**A request has four parts:**

| Part | Example | Meaning |
|---|---|---|
| Method | `GET` | What you want to do. `GET` = read. `POST` = send/create. `PUT`/`PATCH` = update. `DELETE` = remove. Data engineers use `GET` 90% of the time |
| URL | `http://host/repos/acme/payments/issues?state=all&per_page=2` | *Where* (host + path), plus *query parameters* after `?` that refine the question |
| Headers | `Authorization: Bearer ...` | Metadata: who you are, what format you want back |
| Body | (none for GET) | Data you send, mainly with `POST` |

**A response has three parts:** a **status code** (did it work?), **headers** (metadata about the answer), and a **body** (the data, almost always JSON).

### JSON in one minute

JSON is text that maps directly onto Python types:

| JSON | Python |
|---|---|
| `{"a": 1}` object | `dict` |
| `[1, 2]` array | `list` |
| `"text"`, `12`, `true` | `str`, `int`, `True` |
| `null` | `None` |

Real data nests these (a `user` object inside an `issue`) and contains `null`s everywhere. That second fact causes more
pipeline bugs than almost anything else (lesson 9).

### Status codes: who is at fault, and what to do

| Range | Meaning | Examples | Your move |
|---|---|---|---|
| 2xx | Success | `200 OK`, `304 Not Modified` | Use the data |
| 4xx | **You** did something wrong | `401` not authenticated, `403` forbidden, `404` not found, `422` invalid, `429` too many requests | Fix the request. Retrying the identical request fails identically, *except* `429` (slow down) |
| 5xx | **They** failed | `500`, `502`, `503`, `504` | Usually transient: retry with care (lesson 4) |

### The headers that matter in data engineering

| Header | Direction | Why you care | Lesson |
|---|---|---|---|
| `Authorization` | request | Proves who you are | 2 |
| `Link` | response | Where the next page is | 3 |
| `Retry-After` | response | How long to wait | 4 |
| `X-RateLimit-Remaining` / `-Reset` | response | Your remaining budget | 5 |
| `ETag` / `If-None-Match` | both | "Has this changed?" | 8 |
| `Content-Type` | both | The body's format | here |

### Two kinds of failure

1. The server answers with a bad status code (404, 500...). You have a response to inspect.
2. **No response at all:** connection refused, DNS failure, timeout, connection reset. Libraries raise an *exception*, not a status.

A pipeline that only checks status codes crashes on the second kind. A pipeline that only catches exceptions
happily processes the error JSON of the first. Handle both. And **always set a timeout**: Python's popular `requests`
library waits forever by default, so one dead connection can freeze a scheduled job indefinitely.

## Run it

```bash
python api-course/01_http/demo.py
```

**Before running, predict:** what will the status be for a request with no token? For a wrong URL? Which headers will the response contain?

### What you'll see (annotated)

```
  method : GET
  url    : http://127.0.0.1:33801/repos/acme/payments/issues?state=all&per_page=2      <- query params after ?
  header : Authorization: Bearer dev…(hidden)                                           <- secret, masked
...
  status : 200 OK
  header : etag: "d13a9e42..."                       <- fingerprint (lesson 8)
  header : x-ratelimit-remaining: 59                 <- budget left (lesson 5)
  header : link: <http://...&page=2>; rel="next"     <- pagination pointer (lesson 3)
...
  top level is a list with 2 items (we asked for per_page=2)
  nested object   : user = 'user11' (a dict inside the dict)
  null -> None    : closed_at = None   (open issues have no close time)

  valid request  -> 200
  no token       -> 401  '{"message": "Bad credentials"}'
  wrong URL      -> 404  '{"message": "Not Found"}'
  raised ConnectError: this is an exception, not a status code
```

(The port number differs on your machine; the rest is identical.)

## In real jobs

- **REST** (resources as URLs + HTTP verbs) is by far the most common style, and what this course uses.
- **GraphQL** exposes one endpoint where you send a query describing the fields you want. Its rate limits are usually based on query *cost*, not request count.
- **Bulk/file APIs:** for large volumes, many vendors instead hand you a signed link to a file (CSV/Parquet in S3). An API call just tells you where to download.
- Always read the provider's docs for: auth method, pagination style, rate limits, and how to fetch only changes.

## Try it yourself

1. In `demo.py`, change `per_page` to `1`. How does the `Link` header change?
2. Request a path like `/repos/acme/payments/issues/abc`. What status do you get, and is it your fault or theirs?
3. Open the response's `etag` header in two consecutive calls. Same value? Why?

## Check yourself

1. A request returns `503`. Whose problem is it, and is retrying sensible? What about `404`?
2. Why can't you write `if response.status_code == 200:` as your *only* error handling?
3. What happens to `null` in JSON when parsed by Python, and why is that dangerous for a pipeline?

<details><summary>Answers</summary>

1. `503` is the server's problem and is usually temporary, so retrying (with a pause) is sensible. `404` is a problem with your request (wrong URL or resource), so retrying the same request won't help.
2. Network failures (timeouts, refused connections) produce *exceptions* with no status code at all, so a status check alone never sees them. And non-200 success codes like `304` are valid outcomes, not errors.
3. It becomes `None`. Code like `issue["closed_at"].strftime(...)` or arithmetic on a null field crashes, or worse, silently produces wrong results. Real APIs return nulls in "required-looking" fields, so a pipeline must model missing values explicitly.
</details>

**Next:** [Lesson 2: Authentication and secrets](../02_auth/README.md)
