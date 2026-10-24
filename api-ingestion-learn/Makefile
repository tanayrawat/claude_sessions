PORT ?= 8099
# These targets talk to the LOCAL MOCK only. They deliberately force a fake token (:=) so a real GITHUB_TOKEN
# from your shell is never sent to a local server. For real GitHub, call `python -m ghpipe run` directly (see README).
export GITHUB_TOKEN := dev-token
export API_BASE_URL := http://127.0.0.1:$(PORT)
export DATA_DIR ?= data

.PHONY: install test mock mock-flaky run runs replay mutate mutate-bad sql clean

install:            ## deps
	pip install -r requirements.txt

test:               ## full test suite (spins up its own mock server; no network needed)
	python -m pytest -q

mock:               ## well-behaved fake GitHub on :8099
	python -m ghpipe.mock_server --port $(PORT)

mock-flaky:         ## hostile fake GitHub: 15% random 429/500/502 and only 40 requests per 10s
	python -m ghpipe.mock_server --port $(PORT) --fault-rate 0.15 --rate-limit 40 --window 10

run:                ## one incremental pipeline run
	python -m ghpipe run

runs:               ## run history (ops.pipeline_runs)
	python -m ghpipe runs

replay:             ## rebuild silver+gold from bronze, no API calls
	python -m ghpipe replay

mutate:             ## simulate time passing on the source: edits, closes, new issues
	curl -s -XPOST "$(API_BASE_URL)/_admin/mutate"; echo

mutate-bad:         ## same, plus malformed rows and a schema-drifted row
	curl -s -XPOST "$(API_BASE_URL)/_admin/mutate?bad=1"; echo

sql:                ## poke at the warehouse (needs the duckdb CLI: pip install duckdb-cli, or use python)
	python -c "import duckdb,sys; con=duckdb.connect('$(DATA_DIR)/warehouse.duckdb'); [print(con.sql(q)) for q in ['SELECT * FROM gold.open_backlog','SELECT * FROM gold.label_resolution ORDER BY issues DESC LIMIT 8','SELECT check_name, passed, detail FROM ops.dq_results ORDER BY rowid DESC LIMIT 6']]"

clean:
	rm -rf $(DATA_DIR)
