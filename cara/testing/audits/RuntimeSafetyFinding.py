"""RuntimeSafetyFinding."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RuntimeSafetyFinding:
    """One runtime-safety defect: an unbounded HTTP call or an except rebind."""

    path: str
    line: int
    message: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.message}"
