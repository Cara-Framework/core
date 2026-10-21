"""Canonical query-expression classification and identifier validation."""

from __future__ import annotations

import re

from cara.eloquent.expressions import F, Greatest, Least, Operation, QueryExpression
from cara.exceptions import InvalidArgumentException

ORDER_BY_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def _is_column_expression(value) -> bool:
    """Whether a value renders as a quoted column expression, never a bind."""
    return isinstance(value, (F, Operation, Greatest, Least))


def _comparison_expression(column, operator, value, *, keyword=None) -> QueryExpression:
    """The one ``column <operator> value`` predicate ``where`` and ``or_where`` build.

    SQL has no comparison with NULL — ``col != NULL`` and ``col > NULL`` are
    never true — so a ``None`` value maps by operator: ``=`` is ``IS NULL``,
    ``!=``/``<>`` is ``IS NOT NULL``, and any other operator is refused
    rather than collapsed to ``IS NULL``, which silently inverted ``!=`` and
    matched only NULL rows for an ordering comparison.
    """
    if value is None:
        if operator == "=":
            return QueryExpression(column, "=", None, "NULL", keyword=keyword)
        if operator in ("!=", "<>"):
            return QueryExpression(column, "=", True, "NOT NULL", keyword=keyword)
        raise InvalidArgumentException(
            "where(col, op, None): only =, !=, <> compare to NULL; "
            "use where_null/where_not_null"
        )
    return QueryExpression(column, operator, value, "value", keyword=keyword)
