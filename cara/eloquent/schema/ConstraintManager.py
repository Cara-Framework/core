from __future__ import annotations

import re

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401


class ConstraintManager:
    """Single Responsibility: Manages table constraints and indexes"""

    def __init__(self, table):
        self.table = table

    @staticmethod
    def _slugify(text) -> str:
        """Collapse arbitrary text into a snake_case identifier slug.

        Used to auto-derive a CHECK constraint name from its expression
        (``current_price >= 0`` → ``current_price_0``). Non-alphanumeric runs
        become single underscores; leading/trailing underscores are trimmed.
        """
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", str(text)).strip("_").lower()
        return slug or "check"

    def add_check_constraint(self, expression, name=None) -> Self:
        """Add a CHECK constraint carrying a raw boolean SQL expression.

        The expression is rendered verbatim by the platform inside
        ``CONSTRAINT <name> CHECK (<expression>)``. When ``name`` is omitted
        it auto-derives as ``<table>_<slug-of-expression>_check``.
        """
        self.table.add_constraint(
            name or f"{self.table.name}_{self._slugify(expression)}_check",
            "check",
            expression=expression,
        )
        return self

    def add_primary_key(self, columns, name=None) -> Self:
        """Add primary key constraint"""
        if not isinstance(columns, list):
            columns = [columns]

        self.table.add_constraint(
            name or f"{self.table.name}_{'_'.join(columns)}_primary",
            "primary_key",
            columns=columns,
        )
        return self

    def add_unique_constraint(self, columns, name=None, where=None) -> Self:
        """Add unique constraint.

        ``where`` is an optional partial-index predicate. When supplied the
        platform emits a standalone
        ``CREATE UNIQUE INDEX <name> ON <table> (...) WHERE <where>``
        (Postgres partial unique index — the "unique only among active /
        non-deleted rows" pattern) instead of an inline table-level UNIQUE
        constraint. When omitted the plain UNIQUE behaviour is unchanged.
        """
        if not isinstance(columns, list):
            columns = [columns]

        self.table.add_constraint(
            name or f"{self.table.name}_{'_'.join(columns)}_unique",
            "unique",
            columns=columns,
            where=where,
        )
        return self

    def add_index(self, columns, name=None) -> Self:
        """Add index"""
        if not isinstance(columns, list):
            columns = [columns]

        self.table.add_index(
            columns,
            name or f"{self.table.name}_{'_'.join(columns)}_index",
            "index",
        )
        return self

    def add_fulltext_index(self, columns, name=None) -> Self:
        """Add fulltext index"""
        if not isinstance(columns, list):
            columns = [columns]

        self.table.add_constraint(
            name or f"{'_'.join(columns)}_fulltext",
            "fulltext",
            columns,
        )
        return self

    def add_foreign_key(self, column, name=None):
        """Add foreign key constraint.

        ``column`` is either a single column name (scalar FK, unchanged) or a
        list of column names (composite FK). For the composite case the
        auto-generated constraint name joins the columns the same way the
        unique/primary helpers above do, so a two-column FK on ``(a, b)``
        defaults to ``<table>_a_b_foreign``.
        """
        if isinstance(column, list):
            default_name = f"{self.table.name}_{'_'.join(column)}_foreign"
        else:
            default_name = f"{self.table.name}_{column}_foreign"
        return self.table.add_foreign_key(
            column,
            name=name or default_name,
        )
