"""
BetweenExpression - Simple expression class for BETWEEN conditions

Handles BETWEEN clauses in a clean, simple way.
"""


class BetweenExpression:
    """
    Simple expression for BETWEEN clauses.

    Represents a BETWEEN condition in SQL.
    """

    def __init__(self, column: str, min_value, max_value, not_between: bool = False):
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.not_between = not_between

    def __str__(self) -> str:
        """String representation of the BETWEEN expression."""
        operator = "NOT BETWEEN" if self.not_between else "BETWEEN"
        return f"{self.column} {operator} {self.min_value} AND {self.max_value}"

    def __repr__(self) -> str:
        """Developer representation."""
        return f"BetweenExpression(column='{self.column}', min_value='{self.min_value}', max_value='{self.max_value}', not_between={self.not_between})"

    def to_sql(self) -> str:
        """Convert to SQL string."""
        return str(self)
