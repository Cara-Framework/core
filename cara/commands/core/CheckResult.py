"""Typed outcome for one production-readiness check."""

from __future__ import annotations

from dataclasses import dataclass

OK = "ok"
WARN = "warn"
FAIL = "fail"


@dataclass(frozen=True)
class CheckResult:
    """The outcome of a single preflight check."""

    status: str
    message: str

    @property
    def failed(self) -> bool:
        return self.status == FAIL

    @property
    def warned(self) -> bool:
        return self.status == WARN
