"""BarrelPlan."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class BarrelPlan:
    """Result of one generator pass."""

    changed: list[str] = field(default_factory=list)
    collisions: list[str] = field(default_factory=list)
