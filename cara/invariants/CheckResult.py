"""What one invariant check returns: the broken subjects and its cursor."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .Violation import Violation


@dataclass(frozen=True)
class CheckResult:
    """What one check found, and the resume point it leaves (or None)."""

    violations: list[Violation]
    cursor: dict[str, Any] | None = None


__all__ = ["CheckResult"]
