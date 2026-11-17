"""Invariants — layer barrel (generated, DOCTRINE §5.1).

A product-blind invariant auditor: a catalog of rules, one check per rule,
one finding per broken subject with an open/close lifecycle, freshness and
gauges. The product owns the catalog, the checks and the two tables.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "CheckResult": (".CheckResult", "CheckResult"),
    "DEFAULT_CHECK_TIMEOUT_MS": (".InvariantAuditor", "DEFAULT_CHECK_TIMEOUT_MS"),
    "DEFAULT_CLOSED_FINDING_RETENTION_DAYS": (
        ".InvariantAuditor",
        "DEFAULT_CLOSED_FINDING_RETENTION_DAYS",
    ),
    "DEFAULT_RUN_RETENTION_DAYS": (".InvariantAuditor", "DEFAULT_RUN_RETENTION_DAYS"),
    "EVIDENCE_LIMIT": (".Violation", "EVIDENCE_LIMIT"),
    "Invariant": (".Invariant", "Invariant"),
    "InvariantAuditor": (".InvariantAuditor", "InvariantAuditor"),
    "InvariantCatalog": (".InvariantCatalog", "InvariantCatalog"),
    "InvariantCheck": (".InvariantAuditor", "InvariantCheck"),
    "InvariantFindingStore": (".InvariantFindingStore", "InvariantFindingStore"),
    "InvariantMetrics": (".InvariantMetrics", "InvariantMetrics"),
    "OUTCOMES": (".InvariantFindingStore", "OUTCOMES"),
    "OUTCOME_FAILED": (".InvariantFindingStore", "OUTCOME_FAILED"),
    "OUTCOME_HELD": (".InvariantFindingStore", "OUTCOME_HELD"),
    "OUTCOME_VIOLATED": (".InvariantFindingStore", "OUTCOME_VIOLATED"),
    "SEVERITIES": (".Invariant", "SEVERITIES"),
    "SEVERITY_CRITICAL": (".Invariant", "SEVERITY_CRITICAL"),
    "SEVERITY_INFO": (".Invariant", "SEVERITY_INFO"),
    "SEVERITY_WARNING": (".Invariant", "SEVERITY_WARNING"),
    "STATUS_CLOSED": (".InvariantFindingStore", "STATUS_CLOSED"),
    "STATUS_OPEN": (".InvariantFindingStore", "STATUS_OPEN"),
    "Violation": (".Violation", "Violation"),
    "evidence_row": (".Violation", "evidence_row"),
    "violations_by_scope": (".Violation", "violations_by_scope"),
}

__all__ = [
    "CheckResult",
    "DEFAULT_CHECK_TIMEOUT_MS",
    "DEFAULT_CLOSED_FINDING_RETENTION_DAYS",
    "DEFAULT_RUN_RETENTION_DAYS",
    "EVIDENCE_LIMIT",
    "Invariant",
    "InvariantAuditor",
    "InvariantCatalog",
    "InvariantCheck",
    "InvariantFindingStore",
    "InvariantMetrics",
    "OUTCOMES",
    "OUTCOME_FAILED",
    "OUTCOME_HELD",
    "OUTCOME_VIOLATED",
    "SEVERITIES",
    "SEVERITY_CRITICAL",
    "SEVERITY_INFO",
    "SEVERITY_WARNING",
    "STATUS_CLOSED",
    "STATUS_OPEN",
    "Violation",
    "evidence_row",
    "violations_by_scope",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
