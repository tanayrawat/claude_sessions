"""Lesson 10: every earlier lesson, working together. Run:  python api-course/10_capstone_pipeline/demo.py"""
import dataclasses
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import TOKEN, say, start_server, step, title
from ghpipe import bronze, warehouse
from ghpipe.client import ApiClient
from ghpipe.config import Settings
from ghpipe.pipeline import run_pipeline
from ghpipe.quality import DataQualityError

title("Lesson 10 - The whole pipeline: one week in the life of an ingestion job")

server = start_server(n_issues=250)
data_dir = Path(tempfile.mkdtemp())
settings = Settings(base_url=server.url, token=TOKEN, data_dir=data_dir)
client = ApiClient(server.url, TOKEN, sleep=lambda s: None)


def run(label, s=settings, expect_failure=False):
    try:
        r = run_pipeline(s, client)
        print(f"  {label:<44} -> {r.status:<9} fetched={r.fetched:<4} inserted={r.inserted:<4} updated={r.updated:<3} "
              f"rejected={r.rejected} http={r.requests}")
    except DataQualityError as e:
        print(f"  {label:<44} -> BLOCKED by data-quality gate: {str(e)[:60]}")


step("Monday-Thursday: the job runs on a schedule while the source system lives its life")
run("Mon 09:00  first run (full load)")                       # lessons 3, 6, 9
run("Mon 09:15  nothing changed yet")                          # lesson 8 (first poll after full load has no ETag yet)
run("Mon 09:30  nothing changed (now a cheap 304)")            # lesson 8
server.faults = [502, 429]
server.mutate()
run("Tue 09:00  8 edits + 4 new, API hiccups twice")           # lessons 4, 6, 7
server.mutate(bad=True)
run("Wed 09:00  source sends 2 malformed records")             # lesson 9: the gate decides
run("Wed 09:30  operator accepts up to 50% rejects", dataclasses.replace(settings, max_reject_rate=0.5))

step("What the run log (ops.pipeline_runs) tells whoever is on call")
con = warehouse.connect(settings.db_path)
for r in con.execute("""SELECT status, rows_fetched, rows_inserted, rows_updated, rows_rejected, http_requests, http_retries,
                               left(coalesce(error, ''), 48) FROM ops.pipeline_runs ORDER BY started_at""").fetchall():
    print("  ", r)

step("Dead letters: the rows that failed the contract, with the reason")
for r in con.execute("SELECT error FROM silver.dead_letter ORDER BY error").fetchall():
    print("  ", r[0])

step("Gold: what a dashboard would read")
print("  ", con.execute("SELECT * FROM gold.open_backlog").fetchall())
print("  ", con.execute("SELECT label, issues, median_hours_to_close FROM gold.label_resolution ORDER BY issues DESC LIMIT 3").fetchall())

step("Friday: someone finds a bug in the transformation. Rebuild everything from bronze - no API calls")
server.stop()  # the source is gone; we don't need it
before = con.execute("SELECT count(*) FROM silver.issues").fetchall()[0][0]
stats = warehouse.rebuild_silver(con, bronze.all_files(settings.bronze_dir))
after = con.execute("SELECT count(*) FROM silver.issues").fetchall()[0][0]
print(f"  silver rows before: {before}, after replay: {after}  (rebuilt from {len(bronze.all_files(settings.bronze_dir))} saved files)")
con.close()
say("Each line of that story is one of the lessons. When the design feels like a lot, remember each piece exists because "
    "of one specific production failure that you have now watched happen on purpose.")
