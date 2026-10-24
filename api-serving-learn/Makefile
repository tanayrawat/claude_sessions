export WEBHOOK_SECRET ?= s3cret
export API_KEYS ?= demo:demo-key,bi:bi-key
PORT ?= 8000

.PHONY: install test seed serve worker webhooks clean

install:
	pip install -r requirements.txt

test:      ## 17 tests, no server needed
	python -m pytest -q

seed:      ## standalone demo warehouse (or serve part 1's: DB_PATH=../api-ingestion-learn/data/warehouse.duckdb make serve)
	python scripts/seed_demo_db.py

serve:     ## API on :8000 — interactive docs at http://127.0.0.1:8000/docs
	uvicorn app.main:app_from_env --factory --port $(PORT)

worker:    ## drain the webhook inbox once (run after `make webhooks`)
	python -m app.worker --once

webhooks:  ## fire 8 signed/duplicate/out-of-order/forged/poison webhooks at the server
	python scripts/send_webhooks.py

clean:
	rm -rf data
