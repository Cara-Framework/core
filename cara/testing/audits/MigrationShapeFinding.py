"""MigrationShapeFinding."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MigrationShapeFinding:
    """One migration file that cannot be run as written."""

    path: str
    line: int
    problem: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.problem}"
