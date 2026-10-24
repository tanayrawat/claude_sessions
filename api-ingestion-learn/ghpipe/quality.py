"""Data-quality gate. Runs after silver load, BEFORE the watermark moves.

error severity -> run fails, watermark stays put, next run re-reads the same window (safe: idempotent).
warn severity  -> recorded and surfaced, run continues.
"""
from __future__ import annotations

from dataclasses import dataclass


class DataQualityError(Exception):
    pass


@dataclass
class Check:
    name: str
    severity: str  # error | warn
    passed: bool
    detail: str


def _scalar(con, sql: str):
    return con.execute(sql).fetchall()[0][0]


def run_checks(con, *, fetched: int, rejected: int, rows_before: int, max_reject_rate: float) -> list[Check]:
    checks: list[Check] = []

    rate = rejected / fetched if fetched else 0.0
    checks.append(Check("reject_rate", "error", rate <= max_reject_rate, f"{rejected}/{fetched} = {rate:.1%} (max {max_reject_rate:.0%})"))

    dupes = _scalar(con, "SELECT count(*) - count(DISTINCT id) FROM silver.issues")
    checks.append(Check("unique_id", "error", dupes == 0, f"{dupes} duplicate ids"))

    bad_order = _scalar(con, "SELECT count(*) FROM silver.issues WHERE updated_at < created_at")
    checks.append(Check("updated_after_created", "error", bad_order == 0, f"{bad_order} rows"))

    inconsistent = _scalar(con, "SELECT count(*) FROM silver.issues WHERE (state = 'closed') <> (closed_at IS NOT NULL)")
    checks.append(Check("closed_state_matches_closed_at", "error", inconsistent == 0, f"{inconsistent} rows"))

    rows_after = _scalar(con, "SELECT count(*) FROM silver.issues")
    checks.append(Check("no_shrink", "error", rows_after >= rows_before, f"{rows_before} -> {rows_after} rows"))

    stale_days = _scalar(con, "SELECT date_diff('day', max(updated_at), current_timestamp::TIMESTAMP) FROM silver.issues")
    checks.append(Check("freshness_7d", "warn", stale_days is None or stale_days <= 7, f"newest record is {stale_days} days old"))

    return checks


def persist(con, run_id: str, checks: list[Check]) -> None:
    con.executemany(
        "INSERT INTO ops.dq_results VALUES (?, ?, ?, ?, ?)",
        [[run_id, c.name, c.severity, c.passed, c.detail] for c in checks],
    )


def enforce(checks: list[Check]) -> None:
    failed = [c for c in checks if c.severity == "error" and not c.passed]
    if failed:
        raise DataQualityError("; ".join(f"{c.name}: {c.detail}" for c in failed))
