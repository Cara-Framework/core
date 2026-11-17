"""ModelIndexFinding."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ModelIndexFinding:
    """An index defect read from the model declarations."""

    table: str
    subject: str
    message: str

    def __str__(self) -> str:
        return f"{self.table}.{self.subject}: {self.message}"
