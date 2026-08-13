"""One ``field · operator · values`` leaf of a filter tree."""

from __future__ import annotations

import json
from typing import Any

__all__ = ["TreeCondition"]


class TreeCondition:
    """A single comparison, in canonical form (see ``FilterTree``)."""

    __slots__ = ("field", "operator", "values")

    def __init__(self, field: str, operator: str, values: tuple[str, ...]) -> None:
        self.field = field
        self.operator = operator
        self.values = values

    def to_wire(self) -> dict[str, Any]:
        return {"f": self.field, "o": self.operator, "v": list(self.values)}

    def sort_key(self) -> str:
        return json.dumps(self.to_wire(), sort_keys=True, separators=(",", ":"))

    def __repr__(self) -> str:  # pragma: no cover - debugging sugar
        return f"<TreeCondition {self.field} {self.operator} {self.values!r}>"
