"""
SelectExpression - Simple expression class for SELECT statements

Handles SELECT column expressions in a clean, simple way.
"""


class SelectExpression:
    """
    Simple expression for SELECT clauses.

    Represents a column selection with optional alias.
    """

    def __init__(self, column: str, alias: str = None):
        self.column = column
        self.alias = alias

    def __str__(self) -> str:
        """String representation of the SELECT expression."""
        if self.alias:
            return f"{self.column} AS {self.alias}"
        return self.column

    def __repr__(self) -> str:
        """Developer representation."""
        return f"SelectExpression(column='{self.column}', alias='{self.alias}')"

    def to_sql(self) -> str:
        """Convert to SQL string."""
        return str(self)
