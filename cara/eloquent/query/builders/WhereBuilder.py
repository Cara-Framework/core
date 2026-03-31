"""
WhereBuilder - Single Responsibility for WHERE clause construction

Handles all WHERE-related query building operations cleanly and efficiently.
Follows DRY and KISS principles.
"""

from typing import Any, List

from cara.eloquent.expressions.BetweenExpression import BetweenExpression
from cara.eloquent.expressions.QueryExpression import QueryExpression
from cara.support.Collection import Collection


class WhereBuilder:
    """
    Single responsibility: Build WHERE clauses for queries.

    This builder handles:
    - Basic WHERE conditions
    - Complex WHERE logic (AND, OR)
    - IN/NOT IN conditions
    - NULL/NOT NULL conditions
    - BETWEEN conditions
    - EXISTS/NOT EXISTS
    - Date conditions
    - Raw WHERE conditions
    """

    def __init__(self):
        self._wheres = []
        self._bindings = []

    # ===== Basic WHERE =====

    def where(self, column: str, *args) -> "WhereBuilder":
        """Add a basic WHERE condition."""
        operator, value = self._extract_operator_value(*args)

        self._wheres.append(
            QueryExpression(
                column=column,
                equality=operator,
                value=value,
                value_type="value",
                keyword="AND",
            )
        )

        if value is not None:
            self._bindings.append(value)

        return self

    def or_where(self, column: str, *args) -> "WhereBuilder":
        """Add an OR WHERE condition."""
        operator, value = self._extract_operator_value(*args)

        self._wheres.append(
            QueryExpression(
                column=column,
                equality=operator,
                value=value,
                value_type="value",
                keyword="OR",
            )
        )

        if value is not None:
            self._bindings.append(value)

        return self

    # ===== WHERE IN/NOT IN =====

    def where_in(self, column: str, values: List[Any]) -> "WhereBuilder":
        """Add WHERE IN condition."""
        if not values:
            # Empty list should match nothing
            return self.where_raw("1 = 0")

        # Use Collection for consistent handling
        values_collection = Collection(values)
        cleaned_values = values_collection.filter(lambda x: x is not None).all()

        self._wheres.append(
            QueryExpression(
                column=column,
                equality="IN",
                value=cleaned_values,
                value_type="value_list",
                keyword="AND",
            )
        )

        self._bindings.extend(cleaned_values)
        return self

    def where_not_in(self, column: str, values: List[Any]) -> "WhereBuilder":
        """Add WHERE NOT IN condition."""
        if not values:
            # Empty list should match everything
            return self

        values_collection = Collection(values)
        cleaned_values = values_collection.filter(lambda x: x is not None).all()

        self._wheres.append(
            QueryExpression(
                column=column,
                equality="NOT IN",
                value=cleaned_values,
                value_type="value_list",
                keyword="AND",
            )
        )

        self._bindings.extend(cleaned_values)
        return self

    # ===== NULL CONDITIONS =====

    def where_null(self, column: str) -> "WhereBuilder":
        """Add WHERE column IS NULL condition."""
        self._wheres.append(
            QueryExpression(
                column=column,
                equality="IS NULL",
                value=None,
                value_type="NULL",
                keyword="AND",
            )
        )
        return self

    def where_not_null(self, column: str) -> "WhereBuilder":
        """Add WHERE column IS NOT NULL condition."""
        self._wheres.append(
            QueryExpression(
                column=column,
                equality="IS NOT NULL",
                value=None,
                value_type="NOT NULL",
                keyword="AND",
            )
        )
        return self

    def or_where_null(self, column: str) -> "WhereBuilder":
        """Add OR WHERE column IS NULL condition."""
        self._wheres.append(
            QueryExpression(
                column=column,
                equality="IS NULL",
                value=None,
                value_type="NULL",
                keyword="OR",
            )
        )
        return self

    # ===== BETWEEN CONDITIONS =====

    def where_between(self, column: str, low: Any, high: Any) -> "WhereBuilder":
        """Add WHERE BETWEEN condition."""
        self._wheres.append(
            BetweenExpression(
                column=column, min_value=low, max_value=high, not_between=False
            )
        )

        self._bindings.extend([low, high])
        return self

    def where_not_between(self, column: str, low: Any, high: Any) -> "WhereBuilder":
        """Add WHERE NOT BETWEEN condition."""
        self._wheres.append(
            BetweenExpression(
                column=column, min_value=low, max_value=high, not_between=True
            )
        )

        self._bindings.extend([low, high])
        return self

    # ===== RAW CONDITIONS =====

    def where_raw(self, query: str, bindings: tuple = ()) -> "WhereBuilder":
        """Add raw WHERE condition."""
        self._wheres.append(
            QueryExpression(
                column="",
                equality="",
                value=query,
                value_type="RAW",
                keyword="AND",
                raw=True,
                bindings=bindings,
            )
        )

        self._bindings.extend(bindings)
        return self

    # ===== COLUMN COMPARISON =====

    def where_column(
        self, column1: str, column2: str, operator: str = "="
    ) -> "WhereBuilder":
        """Add WHERE column comparison."""
        self._wheres.append(
            QueryExpression(
                column=column1,
                equality=operator,
                value=column2,
                value_type="COLUMN",
                keyword="AND",
            )
        )

        return self

    # ===== Getters =====

    def get_wheres(self) -> List:
        """Get all WHERE conditions."""
        return self._wheres.copy()

    def get_bindings(self) -> List:
        """Get all bindings."""
        return self._bindings.copy()

    def has_wheres(self) -> bool:
        """Check if there are any WHERE conditions."""
        return len(self._wheres) > 0

    # ===== Helper Methods =====

    def _extract_operator_value(self, *args) -> tuple:
        """Extract operator and value from arguments."""
        if len(args) == 1:
            return "=", args[0]
        elif len(args) == 2:
            return args[0], args[1]
        else:
            raise ValueError("Invalid number of arguments for WHERE condition")

    def reset(self) -> "WhereBuilder":
        """Reset all WHERE conditions."""
        self._wheres = []
        self._bindings = []
        return self
