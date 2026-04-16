"""
SelectExpression - Simple expression class for SELECT statements

Handles SELECT column expressions in a clean, simple way.
"""
from typing import Optional


class SelectExpression:
    """Expression for SELECT clauses.

    Represents a column selection with optional alias and raw SQL support.
    """

    def __init__(self, column: str, alias: Optional[str] = None, raw: bool = False):
        self.column = column
        self.alias = alias
        self.raw = raw

    def __str__(self) -> str:
        if self.alias:
            return f"{self.column} AS {self.alias}"
        return self.column

    def __repr__(self) -> str:
        return f"SelectExpression(column='{self.column}', alias={self.alias!r}, raw={self.raw})"

    def to_sql(self) -> str:
        return str(self)
