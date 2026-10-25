# Lesson 3: Pagination

**Time:** 60 min · **Needs:** Lessons 1-2 · **Run:** `python api-course/03_pagination/demo.py`

## The problem

Your source has 250 issues. If the API returned all of them in one response, a source with 50 million rows would
crash the server and your laptop. So APIs return **pages**. This creates two of the most common silent data-loss bugs in
data engineering, because *a pipeline that reads only the first page doesn't crash. It reports success.*

## The three pagination styles

| Style | You send | Next step | Seen in |
|---|---|---|---|
| **Page/offset** | `?page=3&per_page=100` or `?offset=200&limit=100` | Increment the number | Many REST APIs |
| **Cursor** | `?after=<opaque token>` | Use the `next_cursor` from the last response | Stripe, Slack, Twitter, most modern APIs |
| **Link header** | nothing special | Follow the URL in `Link: <...>; rel="next"` | GitHub, GitLab |

Rule: **don't build the next URL yourself if the API provides one.** Follow `next` until it disappears.
The URL carries every filter (`state=all`...), and the provider can change the scheme without breaking you.

## Trap 1: defaults silently hide data

Calling the GitHub issues endpoint with no parameters returns **only open issues, 30 per page.**
Nothing tells you that closed issues exist. You must read the docs and pass `state=all` and a bigger `per_page`.
Every API has a few defaults like this (default time window, default fields, hidden/archived records excluded).

> Habit: after your first load, **reconcile**: compare your row count with a count the provider shows in its UI or a `total_count` field.
> If they differ, find out why *before* anyone builds a dashboard on it.

## Trap 2: data moves while you read it

Pagination isn't a snapshot. While you read page 1, someone edits records. Picture the 250 issues as a list sorted by
`updated_at`. You've read positions 1-100. Now 8 issues are edited. Each edited issue's `updated_at` becomes newest.

- **Oldest-first order:** an edited issue that you've *already read* jumps to the **end** of the list. Everything behind
  it shifts **forward by one**, across your page boundary. The record that slid into the slot you just finished reading is
  never returned. You **skip** it.
- **Newest-first order:** an edited issue jumps to the **front** (page 1, already read). Everything shifts **back by one**, so
  the last row of page 1 reappears at the top of page 2. You read it **twice**.

```
oldest-first, you finished page 1 (positions 1-100), then item A (in page 1) is edited:
 before:  [A B C ... | X Y Z ...]        after:  [B C ... X | Y Z ... A]
                      ^ page 2 starts here               ^ page 2 now starts at Y: X was skipped
newest-first mirror image: a row repeats instead of vanishing.
```

**Duplicates can be cleaned up (lesson 7). A skipped row is gone, and no later run will notice**, because the missed row's
`updated_at` is older than the bookmark you'll store. So: **prefer duplicates over gaps.**

## Run it

**Predict first:** how many rows does the "obvious" call return from a source with 250 issues? In part 3, which order loses data?

```
  rows received: 30   states: ['open']                         <- 30 of 250, and only open ones. No error.
  page 1: 100 rows   Link next -> yes
  page 2: 100 rows   Link next -> yes
  page 3:  50 rows   Link next -> none (last page)
  total 250 / 250

  oldest-first (asc) : duplicates seen =  3   UNTOUCHED rows never seen = 3
  newest-first (desc): duplicates seen =  8   UNTOUCHED rows never seen = 0
```

The measure that matters is the last number: *untouched* rows (never edited) that we never saw. Oldest-first loses 3 for good.
Newest-first loses none, and its 8 repeats are harmless. (In the oldest-first run, the 3 duplicates are edited rows we'd already read that
reappear at the end.)

## In real jobs

- Always write the pagination loop with a **termination guard** (max pages) so a server bug that always returns a `next` link doesn't loop forever.
- Offset pagination also gets *slow* on huge tables (the database counts and discards N rows). Providers who care use cursors.
- Some APIs cap the total depth (e.g. 10,000 results). Then you must partition the query yourself (by date range) to get everything.
- Record `request_url` and page number with each raw page (lesson 9). When numbers look off, you can see what you actually fetched.

## Where it is in the repo

`api-ingestion-learn/ghpipe/client.py` → `paginate()`; `pipeline.py` → the `params` dict with `state=all` and `direction=desc`;
tests `test_state_defaults_to_open_only`, `test_pagination_follows_link_header`.

## Try it yourself

1. In `demo.py` part 2, stop after the first page. How much data did you lose, and did anything warn you?
2. Increase the source to 5,000 issues (`n_issues=5000`). How many pages now? What changes in part 3?
3. Write a loop guard: stop and raise if more than 1,000 pages are fetched.

## Check yourself

1. Why is "follow the `next` link" safer than computing `page+1` yourself?
2. Name two defaults an API might apply that would make your dataset incomplete.
3. Explain, in one sentence each, why oldest-first can lose rows but newest-first cannot.
4. After your first full load, what's one cheap check that you got everything?

<details><summary>Answers</summary>

1. The `next` URL already contains all your filters and the provider's own paging state, and it keeps working if they change the scheme.
2. Examples: only open/active records, a default date window (e.g. last 30 days), default page size, excluding archived/deleted records, hidden fields.
3. Edited rows move toward the end in oldest-first order, which pulls unread rows across your page boundary so one is skipped; in newest-first they move toward the front (already read), which only pushes rows back so you re-read one.
4. Reconcile the row count against a source-provided total or a count shown in the provider's UI, and investigate any difference.
</details>

**Next:** [Lesson 4: Failures and retries](../04_retries/README.md)
