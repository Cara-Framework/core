"""TruthinessFinding."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TruthinessFinding:
    """One zero-corrupting site, reported against a product-relative path."""

    path: str
    line: int
    field: str
    message: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.message}"
