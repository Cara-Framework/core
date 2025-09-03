"""
OnClause - Simple expression class for JOIN ON conditions

Handles ON clauses in a clean, simple way.
"""


class OnClause:
    """
    Simple expression for ON clauses in JOINs.

    Represents an ON condition in a JOIN statement.
    """

    def __init__(
        self,
        first: str,
        operator: str,
        second: str,
        boolean: str = "AND",
        value_type: str = "column",
    ):
        self.first = first
        self.operator = operator
        self.second = second
        self.boolean = boolean
        self.value_type = value_type  # 'column', 'value', 'null', 'not_null'

    def __str__(self) -> str:
        """String representation of the ON clause."""
        if self.value_type in ["null", "not_null"]:
            return f"{self.first} {self.operator}"
        return f"{self.first} {self.operator} {self.second}"

    def __repr__(self) -> str:
        """Developer representation."""
        return f"OnClause(first='{self.first}', operator='{self.operator}', second='{self.second}', boolean='{self.boolean}')"

    def to_sql(self) -> str:
        """Convert to SQL string."""
        return str(self)
