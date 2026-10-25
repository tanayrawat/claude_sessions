# Lesson 8: ETags and conditional requests

**Time:** 30 min · **Needs:** Lessons 3, 5, 6 · **Run:** `python api-course/08_etag/demo.py`

## The problem

You poll a source every 5 minutes, but it changes twice a day. 286 of 288 daily polls download the same data to learn "nothing new", and spend
your rate-limit budget doing it. Can you ask "has this changed?" without downloading the answer?

## The idea

HTTP has a built-in mechanism. The server includes an **`ETag`** header, a fingerprint of the response's content. Next time you send it back in
**`If-None-Match`**:

```
First call   ->  200 OK      ETag: "abc123"     body: 49 KB
Second call  ->  (you send If-None-Match: "abc123")
             ->  304 Not Modified    body: (empty)         <- nothing changed
After an edit -> 200 OK      ETag: "def456"     body: new data
```

Why providers like it: it's cheap for them too. Many (GitHub, for example) **don't count `304` responses against your rate limit**, so polling an unchanged source is free.

The related header is `Last-Modified` / `If-Modified-Since`, a timestamp-based version of the same idea.

## The traps (this is the valuable part)

### Trap 1: an ETag describes one specific response

The fingerprint belongs to *that exact URL with those exact parameters*. In an incremental pipeline, `since=` changes as the watermark moves. So the very first incremental request
differs from the full-load request, and no ETag matches; you need one extra run before 304s start. So you must store the ETag together with the request it came from and only reuse it for an identical request.

### Trap 2: page 1's ETag says nothing about page 3

With a paginated list, the ETag you check is usually only for the first page. If a change lands on a later page, page 1's content is identical, you get `304`, and **you conclude nothing changed. You just missed data.**

```
oldest-first : poll after the edit -> 304   <- WRONG: the change is on page 3, page 1 looks unchanged
newest-first : poll after the edit -> 200   <- correct: new activity always lands on page 1
```

Combine ETag polling with newest-first ordering, so that changes always show up on the page you're fingerprinting.
(This is one more reason lesson 3 recommended newest-first.)

### Not every API supports it

Check for an `ETag` header in real responses before designing around it. Many APIs, especially those behind CDNs that rewrite headers, don't send one, or send weak ones.

## Run it

**Predict:** four plain polls vs four conditional polls (nothing changing): what statuses and total bytes?

```
  without ETag: 4 x 200 OK, 199,044 bytes downloaded
  with ETag   : statuses [200, 304, 304, 304], 49,761 bytes downloaded       <- 75% less data
  quota left  : 95 (the three 304s did not consume any)
```

## In real jobs

- The pattern: "cheap check first, expensive fetch only if changed". You'll see the same idea as `HEAD` requests, change-token endpoints ("what's the latest sync token?"),
  file checksums on S3 (`ETag` there is an object hash), or comparing row counts before a full table copy.
- Combined with the watermark (lesson 6) it gives the pipeline a fast path: `304` ⇒ record "no change", skip the whole load.

## Where it is in the repo

`ghpipe/client.py` (`get(..., etag=)` and the `ApiResponse.not_modified` flag), `ghpipe/pipeline.py` → `extract()` (stores `etag_key`
alongside the etag and reuses it only if the request matches); test `test_etag_does_not_hide_changes_on_later_pages`.
The server half (producing ETags) is lesson 12.

## Try it yourself

1. Poll with the etag, then call `server.mutate()`, then poll again. Which status do you get?
2. Send the same etag with different `per_page`. What happens, and what does that teach about Trap 1?
3. Look at the response headers of a real API you use (`curl -sI https://api.github.com/repos/python/cpython`). Which of these headers are present?

## Check yourself

1. What does a `304` response contain, and why is that valuable?
2. Why can an ETag check wrongly report "no change" on a paginated list?
3. Why is it important to store the ETag with the request it came from?

<details><summary>Answers</summary>

1. Headers only, no body. It saves bandwidth and time, and on many APIs it doesn't consume rate-limit budget.
2. It fingerprints only the first page; changes on later pages leave it unchanged. Sorting so changes appear on page 1 avoids this.
3. An ETag is valid only for the exact request that produced it; sending it with a different URL/parameters can't give a meaningful 304 (or can give a wrong one).
</details>

**Next:** [Lesson 9: Raw data and contracts](../09_raw_and_contracts/README.md)
