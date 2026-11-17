"""The invariant auditor keeps one finding per break and never closes on a
failed check (Synkronus L5, made product-blind)."""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta

import pytest
from prometheus_client import CollectorRegistry

from cara.invariants import (
    OUTCOME_FAILED,
    OUTCOME_HELD,
    OUTCOME_VIOLATED,
    CheckResult,
    Invariant,
    InvariantAuditor,
    InvariantCatalog,
    InvariantFindingStore,
    InvariantMetrics,
    Violation,
    violations_by_scope,
)

NOW = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)


class FakeStore:
    """The store contract in memory (the SQL is exercised by the products'
    Postgres tests)."""

    def __init__(self) -> None:
        self.findings: list[dict] = []
        self.runs: list[dict] = []
        self.timeouts: list[int] = []
        self.pruned: list[tuple] = []

    @contextlib.contextmanager
    def transaction(self):
        yield

    def bound_check_time(self, ms):
        self.timeouts.append(ms)

    def open_findings(self, invariant_id):
        return [
            dict(row)
            for row in self.findings
            if row["invariant"] == invariant_id and row["status"] == "open"
        ]

    def upsert_open(
        self,
        *,
        invariant_id,
        severity,
        owner,
        scope_id,
        subject_key,
        violation_count,
        evidence,
        now,
    ):
        for row in self.findings:
            if (row["invariant"], row["scope_id"], row["subject_key"], row["status"]) == (
                invariant_id,
                scope_id,
                subject_key,
                "open",
            ):
                row.update(violation_count=violation_count, last_seen_at=now)
                return {"id": row["id"], "opened": False}
        row = {
            "id": len(self.findings) + 1,
            "invariant": invariant_id,
            "severity": severity,
            "scope_id": scope_id,
            "subject_key": subject_key,
            "status": "open",
            "violation_count": violation_count,
            "acknowledged": False,
            "opened_at": now,
            "last_seen_at": now,
        }
        self.findings.append(row)
        return {"id": row["id"], "opened": True}

    def close(self, ids, now):
        closed = 0
        for row in self.findings:
            if row["id"] in ids and row["status"] == "open":
                row.update(status="closed", closed_at=now)
                closed += 1
        return closed

    def record_run(self, **run):
        self.runs.append(run)

    def last_good_runs(self):
        good: dict = {}
        for run in self.runs:
            if run["outcome"] != OUTCOME_FAILED:
                good[run["invariant_id"]] = max(
                    good.get(run["invariant_id"], run["started_at"]), run["started_at"]
                )
        return good

    def open_counts(self):
        counts: dict = {}
        for row in self.findings:
            if row["status"] == "open":
                key = (row["invariant"], row["severity"], row["acknowledged"])
                counts[key] = counts.get(key, 0) + 1
        return [
            {"invariant": k[0], "severity": k[1], "acknowledged": k[2], "findings": v}
            for k, v in counts.items()
        ]

    def open_violation_rows(self):
        rows: dict = {}
        for row in self.findings:
            if row["status"] == "open":
                rows[row["invariant"]] = (
                    rows.get(row["invariant"], 0) + row["violation_count"]
                )
        return [{"invariant": k, "rows": v} for k, v in rows.items()]

    def prune(self, runs_before, closed_before):
        self.pruned.append((runs_before, closed_before))
        return {"runs": 0, "findings": 0}


CATALOG = InvariantCatalog(
    ("frequent", "daily"),
    (
        Invariant(
            id="a_holds",
            rule="r",
            severity="critical",
            owner="x",
            cadence="frequent",
            closes_by="c",
        ),
        Invariant(
            id="b_daily",
            rule="r",
            severity="warning",
            owner="x",
            cadence="daily",
            closes_by="c",
        ),
    ),
)


def _metrics():
    return InvariantMetrics.declare("carat", registry=CollectorRegistry())


def _value(metrics, family, **labels):
    return getattr(metrics, family).labels(**labels)._value.get()


def test_a_break_opens_one_finding_refreshes_it_then_closes_when_it_holds():
    store, metrics = FakeStore(), _metrics()
    state = {"broken": True}

    def a_holds(*, now, open_rows):
        if state["broken"]:
            return CheckResult([Violation("rows", 3, [{"id": 1}])])
        return CheckResult([])

    auditor = InvariantAuditor(
        CATALOG,
        {"a_holds": a_holds, "b_daily": lambda **_: CheckResult([])},
        store,
        metrics,
    )
    first = auditor.run("frequent", now=NOW)
    assert first["invariants"]["a_holds"]["outcome"] == OUTCOME_VIOLATED
    assert first["invariants"]["a_holds"]["opened"] == 1
    second = auditor.run("frequent", now=NOW + timedelta(minutes=15))
    assert second["invariants"]["a_holds"]["opened"] == 0
    assert len(store.findings) == 1
    assert (
        _value(
            metrics,
            "findings_open",
            invariant="a_holds",
            severity="critical",
            acknowledged="false",
        )
        == 1
    )
    assert _value(metrics, "violation_rows", invariant="a_holds") == 3

    state["broken"] = False
    third = auditor.run("frequent", now=NOW + timedelta(minutes=30))
    assert third["invariants"]["a_holds"]["outcome"] == OUTCOME_HELD
    assert third["invariants"]["a_holds"]["closed"] == 1
    assert store.findings[0]["status"] == "closed"
    # A cleared break reads 0, not a stale 1.
    assert (
        _value(
            metrics,
            "findings_open",
            invariant="a_holds",
            severity="critical",
            acknowledged="false",
        )
        == 0
    )
    assert store.timeouts and set(store.timeouts) == {120_000}


def test_a_failed_check_closes_nothing_and_keeps_the_cadence_stale():
    store, metrics = FakeStore(), _metrics()
    state = {"mode": "broken"}

    def a_holds(*, now, open_rows):
        if state["mode"] == "boom":
            raise TimeoutError("statement timeout")
        return CheckResult([Violation("rows", 1)])

    auditor = InvariantAuditor(
        CATALOG,
        {"a_holds": a_holds, "b_daily": lambda **_: CheckResult([])},
        store,
        metrics,
    )
    auditor.run("frequent", now=NOW)
    fresh = _value(metrics, "last_success_timestamp_seconds", cadence="frequent")
    assert fresh == NOW.timestamp()

    state["mode"] = "boom"
    report = auditor.run("frequent", now=NOW + timedelta(hours=1))
    assert report["complete"] is False
    assert report["invariants"]["a_holds"]["outcome"] == OUTCOME_FAILED
    assert store.findings[0]["status"] == "open"
    assert store.runs[-1]["outcome"] == OUTCOME_FAILED
    assert _value(metrics, "check_failures_total", invariant="a_holds") == 1
    # The freshness stamp did not move past the failure.
    assert _value(metrics, "last_success_timestamp_seconds", cadence="frequent") == fresh


def test_dry_run_writes_nothing_and_a_scoped_run_closes_only_its_scope():
    store = FakeStore()
    state = {"violations": [Violation("s", 1, scope_id=1), Violation("s", 1, scope_id=2)]}
    auditor = InvariantAuditor(
        CATALOG,
        {
            "a_holds": lambda **_: CheckResult(state["violations"]),
            "b_daily": lambda **_: CheckResult([]),
        },
        store,
    )
    dry = auditor.run("frequent", dry_run=True, now=NOW)
    assert dry["invariants"]["a_holds"]["evidence"] == {"1:s": [], "2:s": []}
    assert store.findings == [] and store.runs == []

    auditor.run("frequent", now=NOW)
    state["violations"] = []
    auditor.run("frequent", scope_id=1, now=NOW + timedelta(minutes=5))
    by_scope = {row["scope_id"]: row["status"] for row in store.findings}
    assert by_scope == {1: "closed", 2: "open"}
    # A one-scope run is not fleet history.
    assert len(store.runs) == 1


def test_prune_runs_only_on_the_prune_cadence():
    store = FakeStore()
    auditor = InvariantAuditor(
        CATALOG,
        {"a_holds": lambda **_: CheckResult([]), "b_daily": lambda **_: CheckResult([])},
        store,
        prune_cadence="daily",
    )
    auditor.run("frequent", now=NOW)
    assert store.pruned == []
    auditor.run("daily", now=NOW)
    assert store.pruned == [(NOW - timedelta(days=30), NOW - timedelta(days=180))]


def test_catalog_and_checks_must_match():
    with pytest.raises(ValueError, match="missing"):
        InvariantAuditor(CATALOG, {"a_holds": lambda **_: None}, FakeStore())
    with pytest.raises(ValueError, match="severity"):
        Invariant(
            id="x", rule="r", severity="loud", owner="o", cadence="c", closes_by="c"
        )
    with pytest.raises(ValueError, match="cadence"):
        InvariantCatalog(("daily",), (CATALOG.get("a_holds"),))
    with pytest.raises(KeyError):
        CATALOG.for_cadence("hourly")


def test_violations_by_scope_keeps_the_full_count_and_twenty_rows():
    rows = [{"scope_id": 7, "id": i, "total": 50} for i in range(30)]
    (item,) = violations_by_scope(rows, "rows")
    assert (item.scope_id, item.count, len(item.evidence)) == (7, 50, 20)
    assert "total" not in item.evidence[0] and "scope_id" not in item.evidence[0]


def test_the_open_index_is_the_upserts_conflict_target():
    scoped = InvariantFindingStore.one_open_index("invariant_finding", "tenant_id")
    assert "(invariant, COALESCE(tenant_id, 0), subject_key)" in scoped["up"]
    plain = InvariantFindingStore.one_open_index()
    assert "(invariant, subject_key) WHERE status = 'open'" in plain["up"]
    with pytest.raises(ValueError):
        InvariantFindingStore(finding_table="x; DROP TABLE y")
