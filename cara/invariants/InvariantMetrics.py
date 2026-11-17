"""The metric families of one product's invariant auditor.

Bounded cardinality: invariant × severity × acknowledged, and cadence. Never
a scope (workspace) or row label: the finding rows name those. Each product
declares its own prefix (``<prefix>_invariant_findings_open`` …), so two
products scraped by one Prometheus never share a series.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cara.observability import REGISTRY, counter, gauge, histogram


@dataclass(frozen=True)
class InvariantMetrics:
    """The five collectors ``InvariantAuditor`` writes."""

    findings_open: Any
    violation_rows: Any
    check_failures_total: Any
    check_duration_seconds: Any
    last_success_timestamp_seconds: Any

    @classmethod
    def declare(cls, prefix: str, registry: Any = REGISTRY) -> InvariantMetrics:
        """Declare (or, after a hot reload, re-find) ``prefix``'s families."""
        return cls(
            findings_open=gauge(
                f"{prefix}_invariant_findings_open",
                "Open invariant findings per invariant, severity and whether an "
                "operator acknowledged them (every label set written each pass).",
                labelnames=("invariant", "severity", "acknowledged"),
                registry=registry,
            ),
            violation_rows=gauge(
                f"{prefix}_invariant_violation_rows",
                "Proving rows behind the open findings of one invariant.",
                labelnames=("invariant",),
                registry=registry,
            ),
            check_failures_total=counter(
                f"{prefix}_invariant_check_failures_total",
                "Invariant checks that could not run (timeout, SQL error, a "
                "dependency down); a failed check closes no finding.",
                labelnames=("invariant",),
                registry=registry,
            ),
            check_duration_seconds=histogram(
                f"{prefix}_invariant_check_duration_seconds",
                "Seconds one invariant check took.",
                labelnames=("invariant",),
                registry=registry,
            ),
            last_success_timestamp_seconds=gauge(
                f"{prefix}_invariant_audit_last_success_timestamp_seconds",
                "Per cadence, the oldest of its invariants' last good check (Unix "
                "time): one check that keeps failing makes the cadence stale.",
                labelnames=("cadence",),
                registry=registry,
            ),
        )


__all__ = ["InvariantMetrics"]
