"""
SelectBuilder - Single Responsibility for SELECT clause construction

Handles all SELECT-related query building operations cleanly and efficiently.
Follows DRY and KISS principles.
"""

from typing import Any, List, Union

from cara.eloquent.expressions.Raw import Raw
from cara.eloquent.expressions.SelectExpression import SelectExpression
from cara.support.Collection import Collection


class SelectBuilder:
    """
    Single responsibility: Build SELECT clauses for queries.

    This builder handles:
    - Column selection
    - Raw SELECT statements
    - Aggregate functions
    - DISTINCT selections
    - Subqueries in SELECT
    """

    def __init__(self):
        self._selects = []
        self._bindings = []
        self._distinct = False

    # ===== Basic SELECT =====

    def select(self, *columns: Union[str, List[str]]) -> "SelectBuilder":
        """Add columns to SELECT clause."""
        if not columns:
            # If no columns specified, select all
            columns = ["*"]

        # Flatten any nested lists using Collection
        flattened = Collection(columns).flatten().all()

        for column in flattened:
            if isinstance(column, str):
                self._selects.append(SelectExpression(column=column, alias=None))

        return self

    def add_select(self, *columns: Union[str, List[str]]) -> "SelectBuilder":
        """Add additional columns to existing SELECT."""
        return self.select(*columns)

    def select_raw(self, query: str, bindings: tuple = ()) -> "SelectBuilder":
        """Add raw SELECT statement."""
        self._selects.append(Raw(query))
        self._bindings.extend(bindings)
        return self

    # ===== Column Aliases =====

    def select_as(self, column: str, alias: str) -> "SelectBuilder":
        """Select column with alias."""
        self._selects.append(SelectExpression(column=column, alias=alias))
        return self

    def alias_columns(self, column_aliases: dict) -> "SelectBuilder":
        """Select multiple columns with aliases."""
        for column, alias in column_aliases.items():
            self.select_as(column, alias)
        return self

    # ===== DISTINCT =====

    def distinct(self, *columns: str) -> "SelectBuilder":
        """Add DISTINCT to query."""
        self._distinct = True

        if columns:
            # DISTINCT on specific columns
            self.select(*columns)

        return self

    def is_distinct(self) -> bool:
        """Check if query has DISTINCT."""
        return self._distinct

    # ===== Aggregate Functions =====

    def count(self, column: str = "*", alias: str = None) -> "SelectBuilder":
        """Add COUNT function."""
        count_expr = f"COUNT({column})"
        if alias:
            count_expr += f" AS {alias}"

        return self.select_raw(count_expr)

    def sum(self, column: str, alias: str = None) -> "SelectBuilder":
        """Add SUM function."""
        sum_expr = f"SUM({column})"
        if alias:
            sum_expr += f" AS {alias}"

        return self.select_raw(sum_expr)

    def avg(self, column: str, alias: str = None) -> "SelectBuilder":
        """Add AVG function."""
        avg_expr = f"AVG({column})"
        if alias:
            avg_expr += f" AS {alias}"

        return self.select_raw(avg_expr)

    def max(self, column: str, alias: str = None) -> "SelectBuilder":
        """Add MAX function."""
        max_expr = f"MAX({column})"
        if alias:
            max_expr += f" AS {alias}"

        return self.select_raw(max_expr)

    def min(self, column: str, alias: str = None) -> "SelectBuilder":
        """Add MIN function."""
        min_expr = f"MIN({column})"
        if alias:
            min_expr += f" AS {alias}"

        return self.select_raw(min_expr)

    # ===== Conditional SELECT =====

    def select_case(
        self, cases: List[tuple], default_value: Any = None, alias: str = None
    ) -> "SelectBuilder":
        """Add CASE statement in SELECT."""
        case_parts = ["CASE"]

        for condition, value in cases:
            case_parts.append(f"WHEN {condition} THEN {value}")

        if default_value is not None:
            case_parts.append(f"ELSE {default_value}")

        case_parts.append("END")

        case_expr = " ".join(case_parts)
        if alias:
            case_expr += f" AS {alias}"

        return self.select_raw(case_expr)

    def select_if_null(
        self, column: str, default_value: Any, alias: str = None
    ) -> "SelectBuilder":
        """Add IFNULL/COALESCE in SELECT."""
        ifnull_expr = f"COALESCE({column}, {default_value})"
        if alias:
            ifnull_expr += f" AS {alias}"

        return self.select_raw(ifnull_expr)

    # ===== String Functions =====

    def select_concat(self, columns: List[str], alias: str = None) -> "SelectBuilder":
        """Add CONCAT function in SELECT."""
        concat_expr = f"CONCAT({', '.join(columns)})"
        if alias:
            concat_expr += f" AS {alias}"

        return self.select_raw(concat_expr)

    def select_substring(
        self, column: str, start: int, length: int = None, alias: str = None
    ) -> "SelectBuilder":
        """Add SUBSTRING function in SELECT."""
        if length is not None:
            substr_expr = f"SUBSTRING({column}, {start}, {length})"
        else:
            substr_expr = f"SUBSTRING({column}, {start})"

        if alias:
            substr_expr += f" AS {alias}"

        return self.select_raw(substr_expr)

    def select_upper(self, column: str, alias: str = None) -> "SelectBuilder":
        """Add UPPER function in SELECT."""
        upper_expr = f"UPPER({column})"
        if alias:
            upper_expr += f" AS {alias}"

        return self.select_raw(upper_expr)

    def select_lower(self, column: str, alias: str = None) -> "SelectBuilder":
        """Add LOWER function in SELECT."""
        lower_expr = f"LOWER({column})"
        if alias:
            lower_expr += f" AS {alias}"

        return self.select_raw(lower_expr)

    # ===== Date Functions =====

    def select_date_format(
        self, column: str, format_str: str, alias: str = None
    ) -> "SelectBuilder":
        """Add DATE_FORMAT function in SELECT."""
        date_format_expr = f"DATE_FORMAT({column}, '{format_str}')"
        if alias:
            date_format_expr += f" AS {alias}"

        return self.select_raw(date_format_expr)

    def select_year(self, column: str, alias: str = None) -> "SelectBuilder":
        """Add YEAR function in SELECT."""
        year_expr = f"YEAR({column})"
        if alias:
            year_expr += f" AS {alias}"

        return self.select_raw(year_expr)

    def select_month(self, column: str, alias: str = None) -> "SelectBuilder":
        """Add MONTH function in SELECT."""
        month_expr = f"MONTH({column})"
        if alias:
            month_expr += f" AS {alias}"

        return self.select_raw(month_expr)

    def select_day(self, column: str, alias: str = None) -> "SelectBuilder":
        """Add DAY function in SELECT."""
        day_expr = f"DAY({column})"
        if alias:
            day_expr += f" AS {alias}"

        return self.select_raw(day_expr)

    # ===== Subqueries =====

    def select_subquery(self, subquery: "QueryBuilder", alias: str) -> "SelectBuilder":
        """Add subquery in SELECT."""
        # This would need the actual QueryBuilder implementation
        subquery_sql = f"({subquery.to_sql()})"
        return self.select_raw(f"{subquery_sql} AS {alias}")

    # ===== Getters =====

    def get_selects(self) -> List:
        """Get all SELECT expressions."""
        return self._selects.copy()

    def get_bindings(self) -> List:
        """Get all bindings."""
        return self._bindings.copy()

    def has_selects(self) -> bool:
        """Check if there are any SELECT expressions."""
        return len(self._selects) > 0

    def get_columns(self) -> List[str]:
        """Get list of selected column names."""
        columns = []
        for select in self._selects:
            if hasattr(select, "column"):
                columns.append(select.column)
        return columns

    def is_selecting_all(self) -> bool:
        """Check if selecting all columns (*)."""
        return any(
            hasattr(select, "column") and select.column == "*" for select in self._selects
        )

    # ===== Helper Methods =====

    def reset(self) -> "SelectBuilder":
        """Reset all SELECT expressions."""
        self._selects = []
        self._bindings = []
        self._distinct = False
        return self

    def clone(self) -> "SelectBuilder":
        """Create a copy of this builder."""
        clone = SelectBuilder()
        clone._selects = self._selects.copy()
        clone._bindings = self._bindings.copy()
        clone._distinct = self._distinct
        return clone

    def merge(self, other: "SelectBuilder") -> "SelectBuilder":
        """Merge another SelectBuilder into this one."""
        self._selects.extend(other._selects)
        self._bindings.extend(other._bindings)

        # If either has distinct, the result should be distinct
        if other._distinct:
            self._distinct = True

        return self
