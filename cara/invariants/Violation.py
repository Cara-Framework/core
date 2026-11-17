"""One broken subject an invariant check returns, and its evidence helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

#: The most proving rows one finding keeps.
EVIDENCE_LIMIT = 20

#: Keys of a proving row that are bookkeeping, not evidence.
_ROW_META = ("scope_id", "tenant_id", "total", "rn")


@dataclass(frozen=True)
class Violation:
    """One broken subject.

    ``scope_id`` partitions findings (a workspace in a multi-tenant product;
    None for a fleet-level subject or a product with no partition).
    ``count`` is the full number of proving rows; ``evidence`` at most
    :data:`EVIDENCE_LIMIT` of them (ids and compared values, no personal data).
    """

    subject_key: str
    count: int
    evidence: list[dict[str, Any]] = field(default_factory=list)
    scope_id: int | None = None


def evidence_row(row: dict[str, Any]) -> dict[str, Any]:
    """``row`` as evidence: bookkeeping keys dropped, Decimals as strings
    (JSON keeps them exact)."""
    return {
        key: (str(value) if isinstance(value, Decimal) else value)
        for key, value in row.items()
        if key not in _ROW_META
    }


def violations_by_scope(
    rows: list[dict[str, Any]], subject_key: str, *, scope_key: str = "scope_id"
) -> list[Violation]:
    """Proving rows grouped into one subject per scope.

    A row may carry ``total`` (a window count over the full set, the usual
    ``COUNT(*) OVER ()``) so a ``LIMIT``-ed read still reports the full
    count; without it the count is the rows read.
    """
    grouped: dict[int | None, list[dict[str, Any]]] = {}
    totals: dict[int | None, int] = {}
    for row in rows:
        raw = row.get(scope_key)
        scope = int(raw) if raw is not None else None
        grouped.setdefault(scope, []).append(evidence_row(row))
        totals[scope] = max(totals.get(scope, 0), int(row.get("total") or 0))
    return [
        Violation(subject_key, totals[scope] or len(items), items[:EVIDENCE_LIMIT], scope)
        for scope, items in grouped.items()
    ]


__all__ = [
    "EVIDENCE_LIMIT",
    "Violation",
    "evidence_row",
    "violations_by_scope",
]
