"""Predicate and JSON constraint construction for ``QueryBuilder``."""

from __future__ import annotations

import inspect
import json
import logging
from typing import Any, Self

from cara.eloquent.expressions import (
    F,
    QueryExpression,
    SelectExpression,
    SubGroupExpression,
    SubSelectExpression,
)
from cara.exceptions import QueryException

from ._QuerySafety import _is_column_expression

_logger = logging.getLogger("cara.eloquent.query")
QueryBuilder: type


def _bind_query_builder(builder_type: type) -> None:
    global QueryBuilder
    QueryBuilder = builder_type


def _qb_select_function_expression(self, func_cls, columns, alias) -> Self:
    """Shared body for ``select_greatest`` / ``select_least``.

    Coerces bare string column names to ``F`` references (so they are
    quoted as identifiers, NOT escaped as string literals) and renders
    the function via the grammar's expression compiler.
    """
    args = [c if _is_column_expression(c) else F(c) for c in columns]
    sql = self._rendering_grammar().compile_expression(func_cls(*args))
    if alias:
        quoted_alias = (
            self._rendering_grammar().column_string().format(column=alias, separator="")
        )
        sql += f" AS {quoted_alias}"
    self._columns += (SelectExpression(sql, raw=True),)
    return self


def _qb_get_processor(self):
    return self.connection_class.get_default_post_processor()()


def _qb_bulk_create(
    self,
    creates: list[dict[str, Any]],
    query: bool = False,
    cast: bool = True,
):
    self.set_action("bulk_create")
    model = None

    if self._model:
        model = self._model

    # First pass: filter / cast each row, but DO NOT sort yet — we
    # need a single canonical column order across all rows so the
    # generated INSERT (column1, column2, …) VALUES (...), (...)
    # actually aligns. The previous implementation sorted each row
    # independently, then BaseGrammar took columns from row[0]
    # only — so heterogeneous rows ({a,b} mixed with {a,c}) silently
    # corrupted: row 2's value for column "c" landed in column "b".
    prepared: list[dict[str, Any]] = []
    column_set: set = set()
    for unsorted_create in creates:
        if model:
            unsorted_create = model.filter_mass_assignment(unsorted_create)
        if cast and model:
            unsorted_create = model.cast_values(unsorted_create)
        prepared.append(unsorted_create)
        column_set.update(unsorted_create.keys())

    # Canonical sorted column list. Missing keys in a row are
    # filled with ``None`` so every row has the same shape under
    # the generated INSERT.
    all_columns = sorted(column_set)
    self._creates = [{col: row.get(col) for col in all_columns} for row in prepared]

    if query:
        return self

    if model:
        model = model.hydrate(self._creates)
    if not self.dry:
        connection = self.new_connection()
        # to_qmark() resets the builder (including _creates); keep the
        # payload for the no-RETURNING fallback.
        creates = self._creates
        query_result = connection.query(self.to_qmark(), self._bindings, results=1)

        processed_results = query_result or creates
    else:
        processed_results = self._creates

    if model:
        return model

    return processed_results


def _qb_create(
    self,
    creates: dict[str, Any] | None = None,
    query: bool = False,
    id_key: str = "id",
    cast: bool = True,
    ignore_mass_assignment: bool = False,
    **kwargs: Any,
) -> Any:
    """
    Create a new record from the given dictionary of values.

    Arguments:
        creates {dict} -- A dictionary of columns and values.

    Returns:
        Model instance (when bound to a model) or raw insert result.
    """
    self.set_action("insert")
    model = None
    self._creates = creates if creates else kwargs

    if self._model:
        model = self._model
        # Update values with related record's
        self._creates.update(self._creates_related)
        # Filter __fillable/__guarded__ fields
        if not ignore_mass_assignment:
            self._creates = model.filter_mass_assignment(self._creates)
        # Cast values if necessary
        if cast:
            self._creates = model.cast_values(self._creates)

    if query:
        return self

    if model:
        model = model.hydrate(self._creates)
        self.observe_events(model, "creating")

        # if attributes were modified during model observer then we need to update the creates here
        self._creates.update(model.get_dirty_attributes())

    if not self.dry:
        connection = self.new_connection()

        # to_qmark() resets the builder (including _creates) once the
        # grammar has been compiled — snapshot the payload first so the
        # no-RETURNING fallback below still has the inserted values.
        creates = self._creates
        query_result = connection.query(self.to_qmark(), self._bindings, results=1)

        if model:
            id_key = model.get_primary_key()

        processed_results = self.get_processor().process_insert_get_id(
            self,
            query_result or creates,
            id_key,
        )
    else:
        processed_results = self._creates

    if model:
        model = model.fill(processed_results)
        self.observe_events(model, "created")
        return model

    return processed_results


def _qb_hydrate(self, result: Any, relations: list[str] | None = None) -> Any:
    return self._model.hydrate(result, relations)


def _qb_delete(
    self, column: str | None = None, value: Any = None, query: bool = False
) -> Self | int:
    """
    Delete rows matching a WHERE clause, or by column/value.

    Keyword Arguments:
        column -- The name of the column (default: {None})
        value -- The value of the column (default: {None})
        query -- If True, return the builder instead of executing.

    Returns:
        Row count affected, or self if query=True.
    """
    model = None
    self.set_action("delete")

    if self._model:
        model = self._model

    # ``value is not None`` — a falsy filter value (0, False, "") is a
    # legitimate predicate; truthiness silently dropped it and the
    # no-WHERE safety net below turned the call into a QueryException.
    if column and value is not None:
        if isinstance(value, (list, tuple)):
            self.where_in(column, value)
        else:
            self.where(column, value)

    if query:
        return self

    if model and model.is_loaded():
        self.where(
            model.get_primary_key(),
            model.get_primary_key_value(),
        )
        self.observe_events(model, "deleting")

    # Safety: refuse to execute DELETE without a WHERE clause to
    # prevent accidental mass-deletion.  Use truncate() instead.
    if not self._wheres:
        raise QueryException(
            "delete() without a WHERE clause would remove all rows. "
            "Use truncate() for intentional mass-deletion."
        )

    result = self.new_connection().query(self.to_qmark(), self._bindings)

    if model:
        self.observe_events(model, "deleted")

    return result


def _qb_where(self, column, *args) -> Self:
    """
    Specifies a where expression.

    Arguments:
        column {string} -- The name of the column to search

    Keyword Arguments:
        args {List} -- The operator and the value of the column to search. (default: {None})

    Returns:
        self
    """
    operator, value = self._extract_operator_value(*args)

    if _is_column_expression(column) or _is_column_expression(value):
        # ``where(F("a"), ">", F("b"))`` (or a literal on either side):
        # both sides are rendered by the grammar's expression compiler so
        # column references stay quoted identifiers and literals are
        # escaped as values — never a bound %s for the expression side.
        self._wheres += (
            (
                QueryExpression(
                    column,
                    operator,
                    value,
                    "expression",
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
                )
            ),
        )
    elif isinstance(column, dict):
        for key, value in column.items():
            self._wheres += ((QueryExpression(key, "=", value, "value")),)
    elif isinstance(value, QueryBuilder):
        self._wheres += (
            (
                QueryExpression(
                    column,
                    operator,
                    SubSelectExpression(value),
                )
            ),
        )
    else:
        self._wheres += ((QueryExpression(column, operator, value, "value")),)
    return self


def _qb_where_from_builder(self, builder) -> Self:
    """
    Specifies a where expression.

    Arguments:
        column {string} -- The name of the column to search

    Keyword Arguments:
        args {List} -- The operator and the value of the column to search. (default: {None})

    Returns:
        self
    """

    self._wheres += ((QueryExpression(None, "=", SubGroupExpression(builder))),)

    return self


def _qb_where_like(self, column, value):
    """
    Specifies a where expression.

    Arguments:
        column {string} -- The name of the column to search

    Keyword Arguments:
        args {List} -- The operator and the value of the column to search. (default: {None})

    Returns:
        self
    """
    return self.where(column, "like", value)


def _qb_where_not_like(self, column, value):
    """
    Specifies a where expression.

    Arguments:
        column {string} -- The name of the column to search

    Keyword Arguments:
        args {List} -- The operator and the value of the column to search. (default: {None})

    Returns:
        self
    """
    return self.where(column, "not like", value)


def _qb_where_raw(self, query: str, bindings=()) -> Self:
    """
    Specifies raw SQL that should be injected into the where expression.

    Arguments:
        query {string} -- The raw query string.

    Keyword Arguments:
        bindings {tuple} -- query bindings that should be added to the connection. (default: {()})

    Returns:
        self
    """
    self._wheres += (
        (
            QueryExpression(
                query,
                "=",
                None,
                "value",
                raw=True,
                bindings=bindings,
            )
        ),
    )
    return self


def _qb_or_where_raw(self, query: str, bindings=()) -> Self:
    """
    Specifies raw SQL that should be injected into the where expression, OR-joined.

    Arguments:
        query {string} -- The raw query string.

    Keyword Arguments:
        bindings {tuple} -- query bindings that should be added to the connection. (default: {()})

    Returns:
        self
    """
    self._wheres += (
        (
            QueryExpression(
                query,
                "=",
                None,
                "value",
                raw=True,
                bindings=bindings,
                keyword="or",
            )
        ),
    )
    return self


def _qb_escape_json_path_segment(segment: str) -> str:
    """Escape a JSON path segment for safe embedding inside single quotes."""
    if not isinstance(segment, str):
        segment = str(segment)
    return segment.replace("'", "''")


def _qb_json_path_sql(column: str, path) -> str:
    """Build a Postgres jsonb path expression ending with ->> (text extract).

    ``path`` may be a dotted string ("a.b.c"), a list, or None/empty for a direct
    column reference. All segments except the last use -> (keep jsonb), last uses
    ->> (cast to text) so the result can be compared against a scalar.
    """
    if path is None or path == "" or path == []:
        return column
    if isinstance(path, str):
        parts = [p for p in path.split(".") if p]
    else:
        parts = [str(p) for p in path if p or p == 0]
    if not parts:
        return column
    esc = [QueryBuilder._escape_json_path_segment(p) for p in parts]
    middle = "".join(f"->'{p}'" for p in esc[:-1])
    return f"{column}{middle}->>'{esc[-1]}'"


def _qb_where_json_contains(self, column: str, value):
    """
    Filter rows whose jsonb column contains the given value (Postgres @>).

    Example:
        Model.where_json_contains("aliases", ["foo"])
        # -> aliases @> '["foo"]'::jsonb

        Model.where_json_contains("metadata", {"featured": True})
        # -> metadata @> '{"featured": true}'::jsonb
    """
    return self.where_raw(f"{column} @> %s::jsonb", [json.dumps(value)])


def _qb_or_where_json_contains(self, column: str, value):
    """OR-joined variant of where_json_contains."""
    return self.or_where_raw(f"{column} @> %s::jsonb", [json.dumps(value)])


def _qb_where_json_doesnt_contain(self, column: str, value):
    """Inverse of where_json_contains."""
    return self.where_raw(f"NOT ({column} @> %s::jsonb)", [json.dumps(value)])


def _qb_where_json_path(self, column: str, path, operator: str = "=", value=None):
    """
    Filter by a nested JSON path extract.

    Arguments:
        column {string} -- The JSON column, e.g. "metadata".
        path {string|list} -- Dotted string ("details.amount") or a list
            of segments. Each segment is treated as a key name, not an array index.
        operator {string} -- SQL comparison operator ("=", ">=", "!=", "LIKE", ...).
        value -- The value to compare against (bound safely).

    Example:
        q.where_json_path("metadata", "external_order_id", "=", "ord_123")
        # -> metadata->>'external_order_id' = %s
    """
    # Two-arg form: where_json_path(column, path, value) with operator defaulted to "="
    if value is None and operator not in (
        "=",
        "!=",
        "<>",
        ">",
        ">=",
        "<",
        "<=",
        "LIKE",
        "ILIKE",
        "NOT LIKE",
        "NOT ILIKE",
    ):
        value = operator
        operator = "="
    sql_col = self._json_path_sql(column, path)
    return self.where_raw(f"{sql_col} {operator} %s", [value])


def _qb_or_where_json_path(self, column: str, path, operator: str = "=", value=None):
    """OR-joined variant of where_json_path."""
    if value is None and operator not in (
        "=",
        "!=",
        "<>",
        ">",
        ">=",
        "<",
        "<=",
        "LIKE",
        "ILIKE",
        "NOT LIKE",
        "NOT ILIKE",
    ):
        value = operator
        operator = "="
    sql_col = self._json_path_sql(column, path)
    return self.or_where_raw(f"{sql_col} {operator} %s", [value])


def _qb_where_json_length(self, column: str, operator_or_value, value=None):
    """
    Filter by length of a jsonb array column.

    Example:
        q.where_json_length("aliases", ">", 0)       # jsonb_array_length(aliases) > 0
        q.where_json_length("aliases", 3)            # jsonb_array_length(aliases) = 3
    """
    if value is None:
        operator, val = "=", operator_or_value
    else:
        operator, val = operator_or_value, value
    return self.where_raw(f"jsonb_array_length({column}) {operator} %s", [val])


def _qb_where_json_key_exists(self, column: str, key: str):
    """
    Filter rows whose jsonb object contains the given top-level key (Postgres ?).

    Example:
        q.where_json_key_exists("metadata", "featured")
        # -> metadata ? %s    (bound: 'featured')

    ``key`` is bound as a parameter — never interpolated — so user-supplied
    keys cannot escape out of the SQL string literal.
    """
    return self.where_raw(f"{column} ? %s", [key])
