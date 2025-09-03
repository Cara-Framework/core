"""
JoinClause - Simple expression class for JOIN statements

Handles JOIN clauses in a clean, simple way.
"""

from typing import List

from ..helpers.misc import deprecated
from .OnClause import OnClause
from .OnValueClause import OnValueClause


class JoinClause:
    """
    Simple expression for JOIN clauses.

    Represents a JOIN operation with table and conditions.
    """

    def __init__(self, join_type: str, table: str, on_clauses: List = None):
        self.join_type = join_type
        self.table = table
        self.on_clauses = on_clauses or []

    def __str__(self) -> str:
        """String representation of the JOIN clause."""
        return f"{self.join_type} JOIN {self.table}"

    def __repr__(self) -> str:
        """Developer representation."""
        return f"JoinClause(join_type='{self.join_type}', table='{self.table}', conditions={len(self.on_clauses)})"

    def to_sql(self) -> str:
        """Convert to SQL string."""
        return str(self)

    def on(self, column1, equality, column2):
        self.on_clauses.append(OnClause(column1, equality, column2))
        return self

    def or_on(self, column1, equality, column2):
        self.on_clauses.append(OnClause(column1, equality, column2, "or"))
        return self

    def on_value(self, column, *args):
        equality, value = self._extract_operator_value(*args)
        self.on_clauses += ((OnValueClause(column, equality, value, "value")),)
        return self

    def or_on_value(self, column, *args):
        equality, value = self._extract_operator_value(*args)
        self.on_clauses += (
            (
                OnValueClause(
                    column,
                    equality,
                    value,
                    "value",
                    operator="or",
                )
            ),
        )
        return self

    def on_null(self, column):
        """
        Specifies an ON expression where the column IS NULL.

        Arguments:
            column {string} -- The name of the column.

        Returns:
            self
        """
        self.on_clauses += ((OnValueClause(column, "=", None, "NULL")),)
        return self

    def on_not_null(self, column: str):
        """
        Specifies an ON expression where the column IS NOT NULL.

        Arguments:
            column {string} -- The name of the column.

        Returns:
            self
        """
        self.on_clauses += ((OnValueClause(column, "=", True, "NOT NULL")),)
        return self

    def or_on_null(self, column):
        """
        Specifies an ON expression where the column IS NULL.

        Arguments:
            column {string} -- The name of the column.

        Returns:
            self
        """
        self.on_clauses += ((OnValueClause(column, "=", None, "NULL", operator="or")),)
        return self

    def or_on_not_null(self, column: str):
        """
        Specifies an ON expression where the column IS NOT NULL.

        Arguments:
            column {string} -- The name of the column.

        Returns:
            self
        """
        self.on_clauses += (
            (
                OnValueClause(
                    column,
                    "=",
                    True,
                    "NOT NULL",
                    operator="or",
                )
            ),
        )
        return self

    @deprecated("Using where() in a Join clause has been superceded by on_value()")
    def where(self, column, *args):
        return self.on_value(column, *args)

    def _extract_operator_value(self, *args):
        operators = [
            "=",
            ">",
            ">=",
            "<",
            "<=",
            "!=",
            "<>",
            "like",
            "not like",
        ]

        operator = operators[0]

        value = None

        if (len(args)) >= 2:
            operator = args[0]
            value = args[1]
        elif len(args) == 1:
            value = args[0]

        if operator not in operators:
            raise ValueError(
                "Invalid comparison operator. The operator can be %s"
                % ", ".join(operators)
            )

        return operator, value

    def get_on_clauses(self):
        return self.on_clauses
