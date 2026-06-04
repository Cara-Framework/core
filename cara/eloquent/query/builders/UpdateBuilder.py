"""
UpdateBuilder - Single Responsibility for UPDATE operations
"""

from __future__ import annotations

from typing import Any

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing_extensions import Self  # noqa: F401


class UpdateBuilder:
    """Single responsibility: Build UPDATE operations for queries."""

    def __init__(self):
        self._table = None
        self._sets = {}
        self._conditions = []
        self._bindings = []

    def table(self, table: str) -> Self:
        """Set the table to update."""
        self._table = table
        return self

    def set(self, column: str, value: Any) -> Self:
        """Set a column value."""
        self._sets[column] = value
        return self

    def where(self, column: str, operator: str = "=", value: Any = None) -> Self:
        """Add WHERE condition."""
        if value is None:
            value = operator
            operator = "="

        self._conditions.append({"column": column, "operator": operator, "value": value})
        self._bindings.append(value)
        return self

    def get_sets(self) -> dict[str, Any]:
        """Get all SET values."""
        return self._sets.copy()

    def get_conditions(self) -> list[dict[str, Any]]:
        """Get all WHERE conditions."""
        return self._conditions.copy()

    def get_bindings(self) -> list[Any]:
        """Get all bindings."""
        return self._bindings.copy()

    def reset(self) -> Self:
        """Reset all UPDATE settings."""
        self._table = None
        self._sets = {}
        self._conditions = []
        self._bindings = []
        return self
