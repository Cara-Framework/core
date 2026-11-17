"""One rule an invariant auditor checks (``InvariantCatalog``)."""

from __future__ import annotations

from dataclasses import dataclass

SEVERITY_CRITICAL = "critical"
SEVERITY_WARNING = "warning"
SEVERITY_INFO = "info"
#: ``critical`` pages, ``warning`` goes to the daily summary, ``info`` is stored.
SEVERITIES = (SEVERITY_CRITICAL, SEVERITY_WARNING, SEVERITY_INFO)


@dataclass(frozen=True)
class Invariant:
    """One rule: what must hold, how bad a break is, who owns it, how often it
    is checked and what closes its finding. The check itself is the product's
    (``InvariantAuditor`` takes it by id)."""

    id: str
    rule: str
    severity: str
    owner: str
    cadence: str
    #: What makes the finding close itself.
    closes_by: str
    #: Minutes a break may last before it counts (None: no window).
    window_minutes: int | None = None
    #: Days a break may last, for a check measured in days.
    window_days: int | None = None

    def __post_init__(self) -> None:
        if self.severity not in SEVERITIES:
            raise ValueError(
                f"invariant {self.id!r}: severity {self.severity!r} is not one of "
                f"{list(SEVERITIES)}"
            )
        if not self.id or not self.id.strip():
            raise ValueError("an invariant needs an id")


__all__ = [
    "SEVERITIES",
    "SEVERITY_CRITICAL",
    "SEVERITY_INFO",
    "SEVERITY_WARNING",
    "Invariant",
]
