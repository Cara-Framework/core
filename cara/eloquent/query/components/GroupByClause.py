from __future__ import annotations

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing_extensions import Self  # noqa: F401

"""
GroupByClause - Simple GROUP BY clause component
"""


class GroupByClause:
    """Simple GROUP BY clause representation."""

    def __init__(self, *columns):
        self.columns = list(columns)

    def add_column(self, column: str) -> Self:
        """Add a column to GROUP BY."""
        self.columns.append(column)
        return self

    def __str__(self) -> str:
        return ", ".join(self.columns)

    def to_sql(self) -> str:
        return str(self)
