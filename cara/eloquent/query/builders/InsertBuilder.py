"""
InsertBuilder - Single Responsibility for INSERT operations
"""

from __future__ import annotations

from typing import Any

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401


class InsertBuilder:
    """Single responsibility: Build INSERT operations for queries."""

    def __init__(self):
        self._table = None
        self._values = []
        self._bindings = []

    def into(self, table: str) -> Self:
        """Set the table to insert into."""
        self._table = table
        return self

    def values(self, data: dict[str, Any]) -> Self:
        """Add values to insert."""
        self._values.append(data)
        return self

    def get_values(self) -> list[dict[str, Any]]:
        """Get all values."""
        return self._values.copy()

    def get_bindings(self) -> list[Any]:
        """Get all bindings."""
        return self._bindings.copy()

    def reset(self) -> Self:
        """Reset all INSERT settings."""
        self._table = None
        self._values = []
        self._bindings = []
        return self
