# Data Engineering Learning Repository

## Start here

**New to APIs in data engineering?** Go to [`api-course/`](api-course/README.md). It's a 13-lesson hands-on course that starts
from "what is an API call?" and builds up, lesson by lesson, to a production-style ingestion pipeline. Each lesson has a short explanation,
a runnable demo that shows a problem breaking on purpose, exercises and self-check questions.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r api-course/requirements.txt
python api-course/01_http/demo.py          # lesson 1; then follow api-course/README.md
```

## What's in the repository

| Folder | What it is | When to use it |
|---|---|---|
| [`api-course/`](api-course/README.md) | **The course.** 13 lessons: HTTP, auth, pagination, retries, rate limits, incremental loads, idempotency, ETags, raw data & contracts, the full pipeline, webhooks, serving, interview practice | Start here and work through it in order |
| [`api-ingestion-learn/`](api-ingestion-learn/README.md) | **Capstone project 1.** A complete GitHub-issues ingestion pipeline (bronze/silver/gold, DQ gate, replay) with a mock server and 20 tests | After lessons 1-9; lesson 10 walks you through it |
| [`api-serving-learn/`](api-serving-learn/README.md) | **Capstone project 2.** A FastAPI data API (cursor paging, auth, rate limits, ETags) and a webhook receiver (signatures, inbox, dedupe) with 17 tests | After lessons 11-12 |
| [`kafka-learn/`](kafka-learn/README.md) | A separate, hands-on introduction to Kafka (streaming). Needs Docker | Optional; unrelated to the API course |

## Suggested order

1. `api-course` lessons 1-9 (about 7 hours)
2. Lesson 10, then read `api-ingestion-learn` and run its tests
3. Lessons 11-12, then explore `api-serving-learn`
4. Lesson 13 (interview practice), repeated weekly until the answers feel natural
