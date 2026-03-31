"""
JoinBuilder - Single Responsibility for JOIN clause construction

Handles all JOIN-related query building operations cleanly and efficiently.
Follows DRY and KISS principles.
"""

from typing import Any, Callable, List

from cara.eloquent.expressions.JoinClause import JoinClause
from cara.eloquent.expressions.OnClause import OnClause


class JoinBuilder:
    """
    Single responsibility: Build JOIN clauses for queries.

    This builder handles:
    - INNER JOIN
    - LEFT JOIN
    - RIGHT JOIN
    - FULL JOIN
    - Cross JOIN
    - Join conditions (ON clauses)
    """

    def __init__(self):
        self._joins = []
        self._bindings = []

    # ===== Basic JOINs =====

    def join(
        self, table: str, first: str, operator: str = "=", second: str = None
    ) -> "JoinBuilder":
        """Add INNER JOIN."""
        return self._add_join("INNER", table, first, operator, second)

    def left_join(
        self, table: str, first: str, operator: str = "=", second: str = None
    ) -> "JoinBuilder":
        """Add LEFT JOIN."""
        return self._add_join("LEFT", table, first, operator, second)

    def right_join(
        self, table: str, first: str, operator: str = "=", second: str = None
    ) -> "JoinBuilder":
        """Add RIGHT JOIN."""
        return self._add_join("RIGHT", table, first, operator, second)

    def inner_join(
        self, table: str, first: str, operator: str = "=", second: str = None
    ) -> "JoinBuilder":
        """Add INNER JOIN (alias for join)."""
        return self.join(table, first, operator, second)

    def full_join(
        self, table: str, first: str, operator: str = "=", second: str = None
    ) -> "JoinBuilder":
        """Add FULL OUTER JOIN."""
        return self._add_join("FULL OUTER", table, first, operator, second)

    def cross_join(self, table: str) -> "JoinBuilder":
        """Add CROSS JOIN."""
        join_clause = JoinClause(join_type="CROSS", table=table, on_clauses=[])

        self._joins.append(join_clause)
        return self

    # ===== Advanced JOINs =====

    def join_where(
        self, table: str, column: str, operator: str, value: Any, join_type: str = "INNER"
    ) -> "JoinBuilder":
        """Add JOIN with WHERE condition."""
        join_clause = JoinClause(join_type=join_type, table=table, on_clauses=[])

        # Add WHERE condition as ON clause
        on_clause = OnClause(
            first=column,
            operator=operator,
            second=value,
            boolean="AND",
            value_type="value",
        )

        join_clause.on_clauses.append(on_clause)
        self._joins.append(join_clause)
        self._bindings.append(value)

        return self

    def join_on_callback(
        self, table: str, callback: Callable, join_type: str = "INNER"
    ) -> "JoinBuilder":
        """Add JOIN with callback for complex ON conditions."""
        join_clause = JoinClause(join_type=join_type, table=table, on_clauses=[])

        # Create a mini builder for ON conditions
        on_builder = JoinOnBuilder()
        callback(on_builder)

        join_clause.on_clauses = on_builder.get_conditions()
        self._joins.append(join_clause)
        self._bindings.extend(on_builder.get_bindings())

        return self

    # ===== Subquery JOINs =====

    def join_subquery(
        self,
        subquery: str,
        alias: str,
        first: str,
        operator: str = "=",
        second: str = None,
        join_type: str = "INNER",
    ) -> "JoinBuilder":
        """Add JOIN with subquery."""
        subquery_table = f"({subquery}) AS {alias}"
        return self._add_join(join_type, subquery_table, first, operator, second)

    def left_join_subquery(
        self,
        subquery: str,
        alias: str,
        first: str,
        operator: str = "=",
        second: str = None,
    ) -> "JoinBuilder":
        """Add LEFT JOIN with subquery."""
        return self.join_subquery(subquery, alias, first, operator, second, "LEFT")

    # ===== Helper Methods =====

    def _add_join(
        self,
        join_type: str,
        table: str,
        first: str,
        operator: str = "=",
        second: str = None,
    ) -> "JoinBuilder":
        """Internal method to add JOIN."""
        if second is None:
            second = operator
            operator = "="

        on_clause = OnClause(
            first=first,
            operator=operator,
            second=second,
            boolean="AND",
            value_type="column",
        )

        join_clause = JoinClause(join_type=join_type, table=table, on_clauses=[on_clause])

        self._joins.append(join_clause)
        return self

    # ===== Getters =====

    def get_joins(self) -> List:
        """Get all JOIN clauses."""
        return self._joins.copy()

    def get_bindings(self) -> List:
        """Get all bindings."""
        return self._bindings.copy()

    def has_joins(self) -> bool:
        """Check if there are any JOINs."""
        return len(self._joins) > 0

    def get_join_count(self) -> int:
        """Get number of JOINs."""
        return len(self._joins)

    def get_join_tables(self) -> List[str]:
        """Get list of joined table names."""
        tables = []
        for join in self._joins:
            if hasattr(join, "table"):
                tables.append(join.table)
        return tables

    def reset(self) -> "JoinBuilder":
        """Reset all JOINs."""
        self._joins = []
        self._bindings = []
        return self


class JoinOnBuilder:
    """
    Helper class for building complex ON conditions in JOINs.
    """

    def __init__(self):
        self._conditions = []
        self._bindings = []

    def on(self, first: str, operator: str, second: str) -> "JoinOnBuilder":
        """Add ON condition for column comparison."""
        condition = OnClause(
            first=first,
            operator=operator,
            second=second,
            boolean="AND",
            value_type="column",
        )

        self._conditions.append(condition)
        return self

    def or_on(self, first: str, operator: str, second: str) -> "JoinOnBuilder":
        """Add OR ON condition."""
        condition = OnClause(
            first=first,
            operator=operator,
            second=second,
            boolean="OR",
            value_type="column",
        )

        self._conditions.append(condition)
        return self

    def on_where(self, column: str, operator: str, value: Any) -> "JoinOnBuilder":
        """Add ON condition with value comparison."""
        condition = OnClause(
            first=column,
            operator=operator,
            second=value,
            boolean="AND",
            value_type="value",
        )

        self._conditions.append(condition)
        self._bindings.append(value)
        return self

    def or_on_where(self, column: str, operator: str, value: Any) -> "JoinOnBuilder":
        """Add OR ON condition with value comparison."""
        condition = OnClause(
            first=column,
            operator=operator,
            second=value,
            boolean="OR",
            value_type="value",
        )

        self._conditions.append(condition)
        self._bindings.append(value)
        return self

    def on_null(self, column: str) -> "JoinOnBuilder":
        """Add ON IS NULL condition."""
        condition = OnClause(
            first=column,
            operator="IS NULL",
            second=None,
            boolean="AND",
            value_type="null",
        )

        self._conditions.append(condition)
        return self

    def on_not_null(self, column: str) -> "JoinOnBuilder":
        """Add ON IS NOT NULL condition."""
        condition = OnClause(
            first=column,
            operator="IS NOT NULL",
            second=None,
            boolean="AND",
            value_type="not_null",
        )

        self._conditions.append(condition)
        return self

    def get_conditions(self) -> List:
        """Get all ON conditions."""
        return self._conditions.copy()

    def get_bindings(self) -> List:
        """Get all bindings."""
        return self._bindings.copy()
