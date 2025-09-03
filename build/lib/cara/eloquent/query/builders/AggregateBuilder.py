"""
AggregateBuilder - Single Responsibility for aggregate functions

Handles all aggregate-related query building operations cleanly and efficiently.
Follows DRY and KISS principles.
"""

from cara.eloquent.expressions.Raw import Raw


class AggregateBuilder:
    """
    Single responsibility: Build aggregate functions for queries.

    This builder handles:
    - COUNT operations
    - SUM operations
    - AVG operations
    - MIN/MAX operations
    - Aggregate expressions
    """

    def __init__(self):
        self._aggregates = []
        self._bindings = []

    # ===== Basic Aggregates =====

    def count(self, column: str = "*", alias: str = None) -> "AggregateBuilder":
        """Add COUNT aggregate."""
        count_expr = f"COUNT({column})"
        if alias:
            count_expr += f" AS {alias}"

        self._aggregates.append(Raw(count_expr))
        return self

    def sum(self, column: str, alias: str = None) -> "AggregateBuilder":
        """Add SUM aggregate."""
        sum_expr = f"SUM({column})"
        if alias:
            sum_expr += f" AS {alias}"

        self._aggregates.append(Raw(sum_expr))
        return self

    def avg(self, column: str, alias: str = None) -> "AggregateBuilder":
        """Add AVG aggregate."""
        avg_expr = f"AVG({column})"
        if alias:
            avg_expr += f" AS {alias}"

        self._aggregates.append(Raw(avg_expr))
        return self

    def max(self, column: str, alias: str = None) -> "AggregateBuilder":
        """Add MAX aggregate."""
        max_expr = f"MAX({column})"
        if alias:
            max_expr += f" AS {alias}"

        self._aggregates.append(Raw(max_expr))
        return self

    def min(self, column: str, alias: str = None) -> "AggregateBuilder":
        """Add MIN aggregate."""
        min_expr = f"MIN({column})"
        if alias:
            min_expr += f" AS {alias}"

        self._aggregates.append(Raw(min_expr))
        return self

    # ===== Advanced Aggregates =====

    def count_distinct(self, column: str, alias: str = None) -> "AggregateBuilder":
        """Add COUNT DISTINCT aggregate."""
        count_expr = f"COUNT(DISTINCT {column})"
        if alias:
            count_expr += f" AS {alias}"

        self._aggregates.append(Raw(count_expr))
        return self

    def sum_distinct(self, column: str, alias: str = None) -> "AggregateBuilder":
        """Add SUM DISTINCT aggregate."""
        sum_expr = f"SUM(DISTINCT {column})"
        if alias:
            sum_expr += f" AS {alias}"

        self._aggregates.append(Raw(sum_expr))
        return self

    def avg_distinct(self, column: str, alias: str = None) -> "AggregateBuilder":
        """Add AVG DISTINCT aggregate."""
        avg_expr = f"AVG(DISTINCT {column})"
        if alias:
            avg_expr += f" AS {alias}"

        self._aggregates.append(Raw(avg_expr))
        return self

    # ===== Statistical Aggregates =====

    def variance(self, column: str, alias: str = None) -> "AggregateBuilder":
        """Add VARIANCE aggregate."""
        var_expr = f"VARIANCE({column})"
        if alias:
            var_expr += f" AS {alias}"

        self._aggregates.append(Raw(var_expr))
        return self

    def stddev(self, column: str, alias: str = None) -> "AggregateBuilder":
        """Add STDDEV aggregate."""
        stddev_expr = f"STDDEV({column})"
        if alias:
            stddev_expr += f" AS {alias}"

        self._aggregates.append(Raw(stddev_expr))
        return self

    # ===== String Aggregates =====

    def group_concat(
        self, column: str, separator: str = ",", alias: str = None
    ) -> "AggregateBuilder":
        """Add GROUP_CONCAT/STRING_AGG aggregate."""
        # Use STRING_AGG for PostgreSQL, GROUP_CONCAT for MySQL
        concat_expr = f"STRING_AGG({column}, '{separator}')"
        if alias:
            concat_expr += f" AS {alias}"

        self._aggregates.append(Raw(concat_expr))
        return self

    # ===== Conditional Aggregates =====

    def count_if(self, condition: str, alias: str = None) -> "AggregateBuilder":
        """Add conditional COUNT aggregate."""
        count_expr = f"COUNT(CASE WHEN {condition} THEN 1 END)"
        if alias:
            count_expr += f" AS {alias}"

        self._aggregates.append(Raw(count_expr))
        return self

    def sum_if(
        self, column: str, condition: str, alias: str = None
    ) -> "AggregateBuilder":
        """Add conditional SUM aggregate."""
        sum_expr = f"SUM(CASE WHEN {condition} THEN {column} END)"
        if alias:
            sum_expr += f" AS {alias}"

        self._aggregates.append(Raw(sum_expr))
        return self

    def avg_if(
        self, column: str, condition: str, alias: str = None
    ) -> "AggregateBuilder":
        """Add conditional AVG aggregate."""
        avg_expr = f"AVG(CASE WHEN {condition} THEN {column} END)"
        if alias:
            avg_expr += f" AS {alias}"

        self._aggregates.append(Raw(avg_expr))
        return self

    # ===== Raw Aggregates =====

    def aggregate_raw(self, expression: str, bindings: tuple = ()) -> "AggregateBuilder":
        """Add raw aggregate expression."""
        self._aggregates.append(Raw(expression))
        self._bindings.extend(bindings)
        return self

    # ===== Getters =====

    def get_aggregates(self) -> list:
        """Get all aggregate expressions."""
        return self._aggregates.copy()

    def get_bindings(self) -> list:
        """Get all bindings."""
        return self._bindings.copy()

    def has_aggregates(self) -> bool:
        """Check if there are any aggregates."""
        return len(self._aggregates) > 0

    def get_aggregate_count(self) -> int:
        """Get number of aggregates."""
        return len(self._aggregates)

    def reset(self) -> "AggregateBuilder":
        """Reset all aggregates."""
        self._aggregates = []
        self._bindings = []
        return self

    def clone(self) -> "AggregateBuilder":
        """Create a copy of this builder."""
        clone = AggregateBuilder()
        clone._aggregates = self._aggregates.copy()
        clone._bindings = self._bindings.copy()
        return clone

    def merge(self, other: "AggregateBuilder") -> "AggregateBuilder":
        """Merge another AggregateBuilder into this one."""
        self._aggregates.extend(other._aggregates)
        self._bindings.extend(other._bindings)
        return self
