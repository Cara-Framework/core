"""One OR/AND group of filter-tree conditions.

The grammar allows conditions only inside a group — depth is capped by
shape, not by a counter (see ``FilterTree``).
"""

from __future__ import annotations

from typing import Any

from .TreeCondition import TreeCondition

__all__ = ["GROUP_ALL", "GROUP_ANY", "TreeGroup"]

GROUP_ANY = "any"
GROUP_ALL = "all"


class TreeGroup:
    """A parenthesized run of conditions joined by one connective."""

    __slots__ = ("connective", "conditions")

    def __init__(self, connective: str, conditions: tuple[TreeCondition, ...]) -> None:
        self.connective = connective
        self.conditions = conditions

    def to_wire(self) -> dict[str, Any]:
        return {self.connective: [condition.to_wire() for condition in self.conditions]}

    def sort_key(self) -> str:
        inner = ",".join(sorted(c.sort_key() for c in self.conditions))
        return f"{self.connective}:[{inner}]"

    def __repr__(self) -> str:  # pragma: no cover - debugging sugar
        return f"<TreeGroup {self.connective} n={len(self.conditions)}>"
