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
        column1: str,
        equality: str,
        column2: str,
        boolean: str = "AND",
        value_type: str = "column",
    ):
        self.column1 = column1
        self.equality = equality
        self.column2 = column2
        self.boolean = boolean
        self.value_type = value_type

    @property
    def first(self):
        return self.column1

    @property
    def operator(self):
        return self.equality

    @property
    def second(self):
        return self.column2

    def __str__(self) -> str:
        """String representation of the ON clause."""
        if self.value_type in ["null", "not_null"]:
            return f"{self.column1} {self.equality}"
        return f"{self.column1} {self.equality} {self.column2}"

    def __repr__(self) -> str:
        """Developer representation."""
        return f"OnClause(column1='{self.column1}', equality='{self.equality}', column2='{self.column2}', boolean='{self.boolean}')"

    def to_sql(self) -> str:
        """Convert to SQL string."""
        return str(self)
