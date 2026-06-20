"""
WhereBuilder - Single Responsibility for WHERE clause construction

Handles all WHERE-related query building operations cleanly and efficiently.
Follows DRY and KISS principles.
"""

from __future__ import annotations

from typing import Any

from cara.eloquent.expressions.BetweenExpression import BetweenExpression
from cara.eloquent.expressions.QueryExpression import QueryExpression
from cara.exceptions import InvalidArgumentException
from cara.support import Collection


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

    def where(self, column: str, *args) -> WhereBuilder:
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

    def or_where(self, column: str, *args) -> WhereBuilder:
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

    def where_in(self, column: str, values: list[Any]) -> WhereBuilder:
        """Add WHERE IN condition."""
        if not values:
            # Empty list should match nothing
            return self.where_raw("1 = 0")

        # Use Collection for consistent handling
        values_collection = Collection(values)
        cleaned_values = values_collection.filter(lambda x: x is not None).all()

        if not cleaned_values:
            # All values were None — emitting ``IN ()`` is a SQL syntax error
            # on Postgres/MySQL. NULL never equals anything (including NULL)
            # in standard SQL, so ``IN (NULL, NULL, …)`` matches nothing
            # anyway. Collapse to the equivalent tautology.
            return self.where_raw("1 = 0")

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

    def where_not_in(self, column: str, values: list[Any]) -> WhereBuilder:
        """Add WHERE NOT IN condition."""
        if not values:
            # Empty exclusion list is almost always a caller bug — e.g.
            # ``Model.where_not_in('id', external_ids).delete()`` where
            # ``external_ids`` came back empty. Silently dropping the
            # clause would turn that into "delete everything". Emit an
            # explicit always-true predicate so the SQL reflects intent
            # ("nothing to exclude") and the query is still well-formed.
            return self.where_raw("1 = 1")

        values_collection = Collection(values)
        cleaned_values = values_collection.filter(lambda x: x is not None).all()

        if not cleaned_values:
            # Same reasoning as ``where_in`` — avoid invalid ``NOT IN ()``.
            return self.where_raw("1 = 1")

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

    def where_null(self, column: str) -> WhereBuilder:
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

    def where_not_null(self, column: str) -> WhereBuilder:
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

    def or_where_null(self, column: str) -> WhereBuilder:
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

    def where_between(self, column: str, low: Any, high: Any) -> WhereBuilder:
        """Add WHERE BETWEEN condition."""
        self._wheres.append(
            BetweenExpression(
                column=column, min_value=low, max_value=high, not_between=False
            )
        )

        self._bindings.extend([low, high])
        return self

    def where_not_between(self, column: str, low: Any, high: Any) -> WhereBuilder:
        """Add WHERE NOT BETWEEN condition."""
        self._wheres.append(
            BetweenExpression(
                column=column, min_value=low, max_value=high, not_between=True
            )
        )

        self._bindings.extend([low, high])
        return self

    # ===== RAW CONDITIONS =====

    def where_raw(self, query: str, bindings: tuple = ()) -> WhereBuilder:
        """Add raw WHERE condition.

        ``BaseGrammar.process_wheres`` reads raw SQL from
        ``expression.column`` (mirroring ``QueryBuilder.where_raw``
        which uses the column slot for the raw query). Storing the
        query in ``value`` made every raw predicate emit an empty SQL
        fragment, so a sentinel like ``where_raw("1 = 0")`` silently
        produced no predicate — and the supposed "match nothing" guard
        for ``where_in([])`` quietly fell back to "match everything".
        """
        self._wheres.append(
            QueryExpression(
                column=query,
                equality="",
                value=None,
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
    ) -> WhereBuilder:
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

    def get_wheres(self) -> list:
        """Get all WHERE conditions."""
        return self._wheres.copy()

    def get_bindings(self) -> list:
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
            raise InvalidArgumentException("Invalid number of arguments for WHERE condition")

    def reset(self) -> WhereBuilder:
        """Reset all WHERE conditions."""
        self._wheres = []
        self._bindings = []
        return self
