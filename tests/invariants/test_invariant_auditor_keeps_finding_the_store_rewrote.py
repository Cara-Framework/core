"""A finding stays open while its subject is broken, whatever key the store wrote.

Live defect: the auditor closed every open finding whose (scope, subject)
pair was not in this pass's violations, comparing the check's raw key with
the row the store had written. ``InvariantFindingStore.upsert_open`` writes
the subject truncated to 160 characters (as text) and, without a scope
column, drops the scope — so a long subject key never matched its own row.
The second pass refreshed the finding and then closed it in the same
transaction; the third reopened it. The open-findings gauge read 0 every
other pass while the break stood, and an acknowledgement was lost each time.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta

from cara.invariants import (
    CheckResult,
    Invariant,
    InvariantAuditor,
    InvariantCatalog,
    Violation,
)

NOW = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)


class _RewritingStore:
    """In-memory store that writes keys the way the SQL store does: subject
    as text cut to 160 characters, scope dropped (no scope column)."""

    def __init__(self) -> None:
        self.findings: list[dict] = []

    @contextlib.contextmanager
    def transaction(self):
        yield

    def bound_check_time(self, ms):
        return None

    def open_findings(self, invariant_id):
        return [
            {"id": r["id"], "scope_id": None, "subject_key": r["subject_key"]}
            for r in self.findings
            if r["invariant"] == invariant_id and r["status"] == "open"
        ]

    def upsert_open(self, *, invariant_id, subject_key, now, **_):
        stored = str(subject_key)[:160]
        for row in self.findings:
            if (row["invariant"], row["subject_key"], row["status"]) == (
                invariant_id,
                stored,
                "open",
            ):
                return {"id": row["id"], "opened": False}
        row = {
            "id": len(self.findings) + 1,
            "invariant": invariant_id,
            "subject_key": stored,
            "status": "open",
        }
        self.findings.append(row)
        return {"id": row["id"], "opened": True}

    def close(self, ids, now):
        closed = 0
        for row in self.findings:
            if row["id"] in ids and row["status"] == "open":
                row["status"] = "closed"
                closed += 1
        return closed

    def record_run(self, **run):
        return None

    def open_count(self) -> int:
        return sum(1 for r in self.findings if r["status"] == "open")


def _auditor(store: _RewritingStore, violation: Violation) -> InvariantAuditor:
    catalog = InvariantCatalog(
        ["hourly"],
        [Invariant("rule", "must hold", "critical", "ops", "hourly", "rule holds")],
    )
    return InvariantAuditor(
        catalog,
        {"rule": lambda **_: CheckResult([violation])},
        store,
    )


def _run_three_passes(violation: Violation) -> tuple[_RewritingStore, list[dict]]:
    store = _RewritingStore()
    auditor = _auditor(store, violation)
    reports = [
        auditor.run("hourly", now=NOW + timedelta(hours=n))["invariants"]["rule"]
        for n in range(3)
    ]
    return store, reports


def test_a_subject_key_longer_than_the_stored_column_keeps_one_open_finding() -> None:
    store, reports = _run_three_passes(Violation("order:" + "x" * 200, 1))

    assert [r["closed"] for r in reports] == [0, 0, 0]
    assert [r["opened"] for r in reports] == [1, 0, 0]
    assert len(store.findings) == 1
    assert store.open_count() == 1


def test_a_scoped_violation_on_a_store_without_a_scope_column_stays_open() -> None:
    store, reports = _run_three_passes(Violation("listing", 2, scope_id=7))

    assert [r["closed"] for r in reports] == [0, 0, 0]
    assert store.open_count() == 1
