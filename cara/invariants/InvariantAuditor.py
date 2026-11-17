"""The invariant auditor: check a catalog, keep one finding per break.

A cadence pass runs every invariant of that cadence. Per invariant: one
bounded read (its own statement timeout) returns the broken subjects with
their proving rows; then, in one transaction, each broken subject's finding
is opened or refreshed and every open finding whose subject now holds is
closed. A check that fails (timeout, SQL error, a dependency down) records a
``failed`` run and touches no finding: a check that did not run never
closes anything.

Gauges are written from the finding table after the pass, every label set
included (zero too), so a cleared break reads 0. Each cadence's freshness
stamp is written last, from the run table: the OLDEST of its invariants'
last good run, so one check that keeps failing makes the whole cadence
stale. Paging is Prometheus only; nothing here sends an alert.

Product-blind: the product gives the catalog, one check per invariant id,
the store (its two tables) and its metric families.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from cara.observability import MetricsBase

from .CheckResult import CheckResult
from .InvariantCatalog import InvariantCatalog
from .InvariantFindingStore import (
    OUTCOME_FAILED,
    OUTCOME_HELD,
    OUTCOME_VIOLATED,
    InvariantFindingStore,
)
from .InvariantMetrics import InvariantMetrics

_logger = logging.getLogger("cara.invariants")

#: ``check(now=..., open_rows=...) -> CheckResult``.
InvariantCheck = Callable[..., CheckResult]

#: Statement timeout of one check (milliseconds).
DEFAULT_CHECK_TIMEOUT_MS = 120_000
#: Run rows are kept this long.
DEFAULT_RUN_RETENTION_DAYS = 30
#: Closed findings are kept this long.
DEFAULT_CLOSED_FINDING_RETENTION_DAYS = 180


def _now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: Any) -> float:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.timestamp()
    return float(value)


class InvariantAuditor:
    """Run a catalog's checks and keep the findings in step."""

    def __init__(
        self,
        catalog: InvariantCatalog,
        checks: Mapping[str, InvariantCheck],
        store: InvariantFindingStore,
        metrics: InvariantMetrics | None = None,
        *,
        check_timeout_ms: int = DEFAULT_CHECK_TIMEOUT_MS,
        prune_cadence: str | None = None,
        run_retention_days: int = DEFAULT_RUN_RETENTION_DAYS,
        closed_finding_retention_days: int = DEFAULT_CLOSED_FINDING_RETENTION_DAYS,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        missing = [entry.id for entry in catalog if entry.id not in checks]
        extra = sorted(set(checks) - set(catalog.ids))
        if missing or extra:
            raise ValueError(
                f"every invariant needs exactly one check: missing {missing}, "
                f"unknown {extra}"
            )
        if prune_cadence is not None and prune_cadence not in catalog.cadences:
            raise ValueError(f"unknown prune cadence {prune_cadence!r}")
        self.catalog = catalog
        self._checks = dict(checks)
        self._store = store
        self._metrics = metrics
        self._timeout_ms = int(check_timeout_ms)
        self._prune_cadence = prune_cadence
        self._run_retention = timedelta(days=run_retention_days)
        self._closed_retention = timedelta(days=closed_finding_retention_days)
        self._clock = clock

    # ─── The pass ─────────────────────────────────────────────────────────

    def run(
        self,
        cadence: str,
        *,
        only: str | None = None,
        scope_id: int | None = None,
        dry_run: bool = False,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Run one cadence pass (or one invariant with ``only``).

        ``scope_id`` keeps one scope's subjects (and closes only its
        findings); ``dry_run`` checks and reports, writing nothing.
        """
        now = now or self._clock()
        entries = (self.catalog.get(only),) if only else self.catalog.for_cadence(cadence)
        report: dict[str, Any] = {"cadence": cadence, "invariants": {}, "complete": True}
        for entry in entries:
            outcome = self._run_one(
                entry, cadence, now=now, scope_id=scope_id, dry_run=dry_run
            )
            report["invariants"][entry.id] = outcome
            if outcome["outcome"] == OUTCOME_FAILED:
                report["complete"] = False
        if not dry_run:
            if cadence == self._prune_cadence and only is None and scope_id is None:
                report["pruned"] = self._store.prune(
                    now - self._run_retention, now - self._closed_retention
                )
            self.publish_gauges()
        return report

    def _run_one(
        self,
        entry: Any,
        cadence: str,
        *,
        now: datetime,
        scope_id: int | None,
        dry_run: bool,
    ) -> dict[str, Any]:
        open_rows = self._store.open_findings(entry.id)
        if scope_id is not None:
            open_rows = [row for row in open_rows if row.get("scope_id") == scope_id]
        started = time.monotonic()
        try:
            with self._store.transaction():
                self._store.bound_check_time(self._timeout_ms)
                result = self._checks[entry.id](now=now, open_rows=open_rows)
        except Exception as exc:
            self._inc(self._metrics and self._metrics.check_failures_total, entry.id)
            _logger.error("invariant check %s failed: %s", entry.id, exc, exc_info=True)
            # A run row is fleet history (freshness, cursor): a one-scope run
            # never writes one.
            if not dry_run and scope_id is None:
                self._store.record_run(
                    invariant_id=entry.id,
                    cadence=cadence,
                    started_at=now,
                    finished_at=now + timedelta(seconds=time.monotonic() - started),
                    outcome=OUTCOME_FAILED,
                    violations=0,
                    cursor=None,
                    error=f"{type(exc).__name__}: {exc}",
                )
            return {"outcome": OUTCOME_FAILED, "error": str(exc)[:300]}
        finally:
            if self._metrics is not None:
                MetricsBase.safe_observe(
                    self._metrics.check_duration_seconds,
                    {"invariant": entry.id},
                    time.monotonic() - started,
                )

        violations = list(result.violations)
        if scope_id is not None:
            violations = [item for item in violations if item.scope_id == scope_id]
        total = sum(int(item.count) for item in violations)
        outcome = OUTCOME_VIOLATED if violations else OUTCOME_HELD
        summary: dict[str, Any] = {
            "outcome": outcome,
            "violations": total,
            "subjects": [
                {
                    "scope_id": item.scope_id,
                    "subject": item.subject_key,
                    "count": item.count,
                }
                for item in violations
            ],
        }
        if dry_run:
            summary["evidence"] = {
                f"{item.scope_id}:{item.subject_key}": item.evidence
                for item in violations
            }
            return summary

        broken = {(item.scope_id, item.subject_key) for item in violations}
        with self._store.transaction():
            opened = 0
            for item in violations:
                written = self._store.upsert_open(
                    invariant_id=entry.id,
                    severity=entry.severity,
                    owner=entry.owner,
                    scope_id=item.scope_id,
                    subject_key=item.subject_key,
                    violation_count=item.count,
                    evidence=item.evidence,
                    now=now,
                )
                opened += 1 if written["opened"] else 0
            holding = [
                int(row["id"])
                for row in open_rows
                if (row.get("scope_id"), row.get("subject_key")) not in broken
            ]
            closed = self._store.close(holding, now)
            if scope_id is None:
                # One scope's subjects only must not move the cursor or the
                # cadence freshness past other scopes' rows.
                self._store.record_run(
                    invariant_id=entry.id,
                    cadence=cadence,
                    started_at=now,
                    finished_at=now + timedelta(seconds=time.monotonic() - started),
                    outcome=outcome,
                    violations=total,
                    cursor=result.cursor,
                    error=None,
                )
        summary.update({"opened": opened, "closed": closed})
        return summary

    # ─── Gauges ───────────────────────────────────────────────────────────

    def publish_gauges(self) -> None:
        """Mirror the open findings into the gauges, every label set written."""
        metrics = self._metrics
        if metrics is None:
            return
        counts = {
            (row["invariant"], row["severity"], bool(row["acknowledged"])): int(
                row["findings"]
            )
            for row in self._store.open_counts()
        }
        rows = {
            row["invariant"]: int(row["rows"] or 0)
            for row in self._store.open_violation_rows()
        }
        for entry in self.catalog:
            for acknowledged in (False, True):
                MetricsBase.safe_set(
                    metrics.findings_open,
                    {
                        "invariant": entry.id,
                        "severity": entry.severity,
                        "acknowledged": "true" if acknowledged else "false",
                    },
                    counts.get((entry.id, entry.severity, acknowledged), 0),
                )
            MetricsBase.safe_set(
                metrics.violation_rows, {"invariant": entry.id}, rows.get(entry.id, 0)
            )
        # A cadence with an invariant that never ran well is left unset (the
        # alert reads it as absent).
        good = self._store.last_good_runs()
        for cadence in self.catalog.cadences:
            stamps = [good.get(entry.id) for entry in self.catalog.for_cadence(cadence)]
            if stamps and all(stamp is not None for stamp in stamps):
                MetricsBase.safe_set(
                    metrics.last_success_timestamp_seconds,
                    {"cadence": cadence},
                    min(_timestamp(stamp) for stamp in stamps),
                )

    @staticmethod
    def _inc(metric: Any, invariant_id: str) -> None:
        if metric is not None:
            MetricsBase.safe_inc(metric, {"invariant": invariant_id})


__all__ = [
    "DEFAULT_CHECK_TIMEOUT_MS",
    "DEFAULT_CLOSED_FINDING_RETENTION_DAYS",
    "DEFAULT_RUN_RETENTION_DAYS",
    "InvariantAuditor",
    "InvariantCheck",
]
