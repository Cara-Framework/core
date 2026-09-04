"""
JoinClause - Simple expression class for JOIN statements.

Handles JOIN clauses in a clean, simple way.
"""

from __future__ import annotations

from typing import Self

from .OnClause import OnClause


class JoinClause:
    """
    Simple expression for JOIN clauses.

    Represents a JOIN operation with table and conditions.
    """

    def __init__(
        self,
        table: str,
        clause: str = "inner",
        on_clauses: list | None = None,
        join_type: str | None = None,
    ):
        self.table = table
        self.clause = join_type or clause
        self.join_type = self.clause
        self.on_clauses = on_clauses or []
        self.alias = None

    def __str__(self) -> str:
        """String representation of the JOIN clause."""
        return f"{self.join_type} JOIN {self.table}"

    def __repr__(self) -> str:
        """Developer representation."""
        return f"JoinClause(join_type='{self.join_type}', table='{self.table}', conditions={len(self.on_clauses)})"

    def to_sql(self) -> str:
        """Convert to SQL string."""
        return str(self)

    def on(self, column1, equality, column2) -> Self:
        self.on_clauses.append(OnClause(column1, equality, column2))
        return self

    def get_on_clauses(self):
        return self.on_clauses
