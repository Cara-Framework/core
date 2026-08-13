"""Predicate variants and relationship-existence constraints for ``QueryBuilder``."""

from __future__ import annotations

import inspect
import inspect as _inspect
import logging
from datetime import datetime
from typing import Self

from cara.eloquent.expressions import (
    BetweenExpression,
    HavingExpression,
    QueryExpression,
    SubGroupExpression,
    SubSelectExpression,
)

from ._QuerySafety import _is_column_expression

_logger = logging.getLogger("cara.eloquent.query")
QueryBuilder: type


def _bind_query_builder(builder_type: type) -> None:
    global QueryBuilder
    QueryBuilder = builder_type


def _qb_or_where(self, column, *args) -> Self:
    """
    Specifies an or where query expression.

    Arguments:
        column {[type]} -- [description]
        value {[type]} -- [description]

    Returns:
        [type] -- [description]
    """
    operator, value = self._extract_operator_value(*args)
    if _is_column_expression(column) or _is_column_expression(value):
        # OR-joined column-reference comparison — see ``where`` for the
        # expression-rendering rationale.
        self._wheres += (
            (
                QueryExpression(
                    column,
                    operator,
                    value,
                    "expression",
                    keyword="or",
                )
            ),
        )
    elif inspect.isfunction(column):
        builder = column(self.new())
        self._wheres += (
            (
                QueryExpression(
                    None,
                    operator,
                    SubGroupExpression(builder),
                    keyword="or",
                )
            ),
        )
    elif isinstance(value, QueryBuilder):
        self._wheres += (
            (
                QueryExpression(
                    column,
                    operator,
                    SubSelectExpression(value),
                    keyword="or",
                )
            ),
        )
    else:
        self._wheres += (
            (
                QueryExpression(
                    column,
                    operator,
                    value,
                    "value",
                    keyword="or",
                )
            ),
        )
    return self


def _qb_where_exists(self, value: str | int | QueryBuilder) -> Self:
    """
    Specifies a where exists expression.

    Arguments:
        value {string|int|QueryBuilder} -- A value to check for the existence of a query expression.

    Returns:
        self
    """
    if inspect.isfunction(value):
        self._wheres += (
            (
                QueryExpression(
                    None,
                    "EXISTS",
                    SubSelectExpression(value(self.new())),
                )
            ),
        )
    elif isinstance(value, QueryBuilder):
        self._wheres += (
            (
                QueryExpression(
                    None,
                    "EXISTS",
                    SubSelectExpression(value),
                )
            ),
        )
    else:
        self._wheres += ((QueryExpression(None, "EXISTS", value, "value")),)

    return self


def _qb_or_where_exists(self, value: str | int | QueryBuilder) -> Self:
    """
    Specifies a where exists expression.

    Arguments:
        value {string|int|QueryBuilder} -- A value to check for the existence of a query expression.

    Returns:
        self
    """
    if inspect.isfunction(value):
        self._wheres += (
            (
                QueryExpression(
                    None,
                    "EXISTS",
                    SubSelectExpression(value(self.new())),
                    keyword="or",
                )
            ),
        )
    elif isinstance(value, QueryBuilder):
        self._wheres += (
            (
                QueryExpression(
                    None,
                    "EXISTS",
                    SubSelectExpression(value),
                    keyword="or",
                )
            ),
        )
    else:
        self._wheres += (
            (
                QueryExpression(
                    None,
                    "EXISTS",
                    value,
                    "value",
                    keyword="or",
                )
            ),
        )

    return self


def _qb_where_not_exists(self, value: str | int | QueryBuilder) -> Self:
    """
    Specifies a where exists expression.

    Arguments:
        value {string|int|QueryBuilder} -- A value to check for the existence of a query expression.

    Returns:
        self
    """

    if inspect.isfunction(value):
        self._wheres += (
            (
                QueryExpression(
                    None,
                    "NOT EXISTS",
                    SubSelectExpression(value(self.new())),
                )
            ),
        )
    elif isinstance(value, QueryBuilder):
        self._wheres += (
            (
                QueryExpression(
                    None,
                    "NOT EXISTS",
                    SubSelectExpression(value),
                )
            ),
        )
    else:
        self._wheres += ((QueryExpression(None, "NOT EXISTS", value, "value")),)

    return self


def _qb_or_where_not_exists(self, value: str | int | QueryBuilder) -> Self:
    """
    Specifies a where exists expression.

    Arguments:
        value {string|int|QueryBuilder} -- A value to check for the existence of a query expression.

    Returns:
        self
    """

    if inspect.isfunction(value):
        self._wheres += (
            (
                QueryExpression(
                    None,
                    "NOT EXISTS",
                    SubSelectExpression(value(self.new())),
                    keyword="or",
                )
            ),
        )
    elif isinstance(value, QueryBuilder):
        self._wheres += (
            (
                QueryExpression(
                    None,
                    "NOT EXISTS",
                    SubSelectExpression(value),
                    keyword="or",
                )
            ),
        )
    else:
        self._wheres += (
            (
                QueryExpression(
                    None,
                    "NOT EXISTS",
                    value,
                    "value",
                    keyword="or",
                )
            ),
        )

    return self


def _qb_having(self, column, equality="", value="") -> Self:
    """
    Specifying a having expression.

    Arguments:
        column {string} -- The name of the column.

    Keyword Arguments:
        equality {string} -- An equality operator (default: {"="})
        value {string} -- The value of the having expression (default: {""})

    Returns:
        self
    """
    self._having += ((HavingExpression(column, equality, value)),)
    return self


def _qb_having_raw(self, string) -> Self:
    """
    Specifies raw SQL that should be injected into the having expression.

    Arguments:
        string {string} -- The raw query string.

    Returns:
        self
    """
    self._having += ((HavingExpression(string, raw=True)),)
    return self


def _qb_where_null(self, column) -> Self:
    """
    Specifies a where expression where the column is NULL.

    Arguments:
        column {string} -- The name of the column.

    Returns:
        self
    """
    self._wheres += ((QueryExpression(column, "=", None, "NULL")),)
    return self


def _qb_or_where_null(self, column) -> Self:
    """
    Specifies a where expression where the column is NULL.

    Arguments:
        column {string} -- The name of the column.

    Returns:
        self
    """
    self._wheres += ((QueryExpression(column, "=", None, "NULL", keyword="or")),)
    return self


def _qb_where_not_null(self, column: str) -> Self:
    """
    Specifies a where expression where the column is not NULL.

    Arguments:
        column {string} -- The name of the column.

    Returns:
        self
    """
    self._wheres += ((QueryExpression(column, "=", True, "NOT NULL")),)
    return self


def _qb_get_date_string(self, date):
    if isinstance(date, str):
        return date
    elif hasattr(date, "to_date_string"):
        return date.to_date_string()
    elif hasattr(date, "strftime"):
        return date.strftime("%Y-%m-%d")


def _qb_where_date(self, column: str, date: str | datetime) -> Self:
    """
    Specifies a where DATE expression.

    Arguments:
        column {string} -- The name of the column.

    Returns:
        self
    """
    self._wheres += (
        (
            QueryExpression(
                column,
                "=",
                self._get_date_string(date),
                "DATE",
            )
        ),
    )
    return self


def _qb_or_where_date(self, column: str, date: str | datetime) -> Self:
    """
    Specifies a where DATE expression.

    Arguments:
        column {string} -- The name of the column.
        date {string|datetime|pendulum} -- The name of the column.

    Returns:
        self
    """
    self._wheres += (
        (
            QueryExpression(
                column,
                "=",
                self._get_date_string(date),
                "DATE",
                keyword="or",
            )
        ),
    )
    return self


def _qb_between(self, column: str, low: int, high: int) -> Self:
    """
    Specifies a where between expression.

    Arguments:
        column {string} -- The name of the column.
        low {string} -- The value on the low end.
        high {string} -- The value on the high end.

    Returns:
        self
    """
    self._wheres += (BetweenExpression(column, low, high),)
    return self


def _qb_where_between(self, *args, **kwargs):
    return self.between(*args, **kwargs)


def _qb_where_not_between(self, *args, **kwargs):
    return self.not_between(*args, **kwargs)


def _qb_not_between(self, column: str, low: str, high: str) -> Self:
    """
    Specifies a where not between expression.

    Arguments:
        column {string} -- The name of the column.
        low {string} -- The value on the low end.
        high {string} -- The value on the high end.

    Returns:
        self
    """
    self._wheres += (BetweenExpression(column, low, high, not_between=True),)
    return self


def _qb_where_in(self, column, wheres=None) -> Self:
    """
    Specifies where a column contains a list of a values.

    Arguments:
        column {string} -- The name of the column.

    Keyword Arguments:
        wheres {list} -- A list of values (default: {[]})

    Returns:
        self
    """

    wheres = wheres or []

    if not wheres:
        self._wheres += ((QueryExpression(0, "=", 1, "value_equals")),)

    elif isinstance(wheres, QueryBuilder):
        self._wheres += (
            (
                QueryExpression(
                    column,
                    "IN",
                    SubSelectExpression(wheres),
                )
            ),
        )
    elif callable(wheres):
        self._wheres += (
            (
                QueryExpression(
                    column,
                    "IN",
                    SubSelectExpression(wheres(self.new())),
                )
            ),
        )
    else:
        # Drop None values. ``IN (NULL, …)`` is never true in standard
        # SQL (NULL ≠ NULL), and the grammar would otherwise splice
        # the Python literal ``'None'`` as a string — a silent type
        # mismatch that always returns zero rows. If every value was
        # None, collapse to the same "match nothing" sentinel as the
        # empty-list branch instead of emitting bogus SQL.
        cleaned = [v for v in wheres if v is not None]
        if not cleaned:
            self._wheres += ((QueryExpression(0, "=", 1, "value_equals")),)
        else:
            self._wheres += ((QueryExpression(column, "IN", cleaned)),)
    return self


def _qb_get_relation(self, relationship, builder=None):
    if not builder:
        builder = self

    if not builder._model:
        raise AttributeError(
            "You must specify a model in order to use relationship methods"
        )

    # ``builder._model`` may be an unhydrated instance — in that case
    # ``getattr(instance, rel)`` triggers the descriptor's instance-path
    # (lazy-load from ``__attributes__``) and KeyErrors on the local key.
    # Resolve via the descriptor on the class (walking the MRO).

    owner = builder._model if _inspect.isclass(builder._model) else type(builder._model)
    rel = owner.__dict__.get(relationship)
    if rel is None:
        for base in owner.__mro__:
            if relationship in base.__dict__:
                rel = base.__dict__[relationship]
                break
    if rel is None:
        raise AttributeError(
            f"Relation '{relationship}' is not defined on {owner.__name__}"
        )
    return rel


def _qb_has(self, *relationships) -> Self:
    if not self._model:
        raise AttributeError(
            "You must specify a model in order to use 'has' relationship methods"
        )

    for relationship in relationships:
        if "." in relationship:
            last_builder = self._model.builder
            for split_relationship in relationship.split("."):
                related = last_builder.get_relation(split_relationship)
                last_builder = related.query_has(last_builder)
        else:
            related = self._resolve_relation_descriptor(relationship)
            related.query_has(self)
    return self


def _qb_or_has(self, *relationships) -> Self:
    if not self._model:
        raise AttributeError(
            "You must specify a model in order to use 'has' relationship methods"
        )

    for relationship in relationships:
        if "." in relationship:
            last_builder = self._model.builder
            split_count = len(relationship.split("."))
            for index, split_relationship in enumerate(relationship.split(".")):
                related = last_builder.get_relation(split_relationship)

                if index + 1 == split_count:
                    last_builder = related.query_has(
                        last_builder,
                        method="where_exists",
                    )
                    continue

                last_builder = related.query_has(
                    last_builder,
                    method="or_where_exists",
                )
        else:
            related = self._resolve_relation_descriptor(relationship)
            related.query_has(self, method="or_where_exists")
    return self
