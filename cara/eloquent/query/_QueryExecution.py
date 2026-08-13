"""Query limits, writes and aggregate execution for ``QueryBuilder``."""

from __future__ import annotations

import logging
from typing import Any, Self

from cara.eloquent.expressions import (
    AggregateExpression,
    GroupByExpression,
    OrderByExpression,
    UpdateQueryExpression,
)
from cara.exceptions import (
    InvalidArgumentException,
    QueryException,
)

from ._QuerySafety import ORDER_BY_COLUMN_RE as _ORDER_BY_COLUMN_RE
from ._QuerySafety import _is_column_expression

_logger = logging.getLogger("cara.eloquent.query")
QueryBuilder: type


def _bind_query_builder(builder_type: type) -> None:
    global QueryBuilder
    QueryBuilder = builder_type


def _qb_limit(self, amount) -> Self:
    """
    Specifies a limit expression.

    Arguments:
        amount {int} -- The number of rows to limit. ``None`` clears
            the limit (same as never calling ``limit``). ``0`` means
            "return zero rows" and is honored; negative values are
            rejected.

    Returns:
        self
    """
    if amount is None:
        self._limit = False
    elif isinstance(amount, bool):
        # ``True``/``False`` would coerce to 1/0 silently — almost
        # always a caller mistake. ``False`` matches the sentinel
        # for "no limit", but pinning it here makes intent explicit.
        raise InvalidArgumentException(f"limit() expects an int or None, got {amount!r}")
    elif isinstance(amount, int):
        if amount < 0:
            raise InvalidArgumentException(f"limit() must be >= 0, got {amount!r}")
        self._limit = amount
    else:
        raise InvalidArgumentException(f"limit() expects an int or None, got {amount!r}")
    return self


def _qb_offset(self, amount) -> Self:
    """
    Specifies an offset expression.

    Arguments:
        amount {int} -- The number of rows to skip. ``None`` clears
            the offset. Negative values are rejected by supported drivers.

    Returns:
        self
    """
    if amount is None:
        self._offset = False
    elif isinstance(amount, bool):
        raise InvalidArgumentException(f"offset() expects an int or None, got {amount!r}")
    elif isinstance(amount, int):
        if amount < 0:
            raise InvalidArgumentException(f"offset() must be >= 0, got {amount!r}")
        self._offset = amount
    else:
        raise InvalidArgumentException(f"offset() expects an int or None, got {amount!r}")
    return self


def _qb_update(
    self,
    updates: dict[str, Any],
    dry: bool = False,
    force: bool = False,
    cast: bool = True,
    ignore_mass_assignment: bool = False,
):
    """
    Specifies columns and values to be updated.

    Arguments:
        updates {dictionary} -- A dictionary of columns and values to update.
        dry {bool, optional}: Do everything except execute the query against the DB
        force {bool, optional}: Force an update statement to be executed even if nothing was changed
        cast {bool, optional}: Run all values through model's casters
        ignore_mass_assignment {bool, optional}: Whether the update should ignore mass assignment on the model

    Returns:
        self
    """
    model = None

    additional = {}

    if self._model:
        model = self._model
        # Filter __fillable/__guarded__ fields
        if not ignore_mass_assignment:
            updates = model.filter_mass_assignment(updates)

    if model and model.is_loaded():
        self.where(
            model.get_primary_key(),
            model.get_primary_key_value(),
        )
        additional.update({model.get_primary_key(): model.get_primary_key_value()})

        self.observe_events(model, "updating")

    if model:
        if not model.__force_update__ and not force:
            # Filter updates to only those with changes. JSON / array /
            # collection casts are EXEMPT from change-detection: their cast
            # value is a mutable dict/list and ``getattr`` hands back the
            # very object stored in ``__original_attributes__`` (the two
            # alias after hydrate), so an in-place mutation makes
            # ``original != value`` compare an object against itself
            # (always False) and the write is SILENTLY dropped — real data
            # loss with no error (CODING_RULES §8). We cannot detect the
            # change reliably, so always persist these columns when they
            # are present in the update: re-writing an unchanged JSON value
            # is cheap; dropping a mutated one is a bug.
            _mutable_casts = ("json", "array", "collection")
            updates = {
                attr: value
                for attr, value in updates.items()
                if (
                    value is None
                    # Column-reference expressions (F / arithmetic /
                    # GREATEST / LEAST) are computed server-side from the
                    # CURRENT row, so there is no Python value to compare
                    # against ``__original_attributes__`` — never drop
                    # them via change-detection.
                    or _is_column_expression(value)
                    or model.__casts__.get(attr) in _mutable_casts
                    or model.__original_attributes__.get(attr, None) != value
                )
            }

        # Do not execute query if no changes
        if not updates:
            return self if dry or self.dry else model

        # Cast date fields
        date_fields = model.get_dates()
        for key, value in updates.items():
            if key in date_fields:
                if value is not None:
                    updates[key] = model.get_new_datetime_string(value)
                else:
                    updates[key] = value
            # Cast value if necessary
            # NOTE: Must use `is not None` — NOT `if value` — because
            # falsy values like {}, [], 0, False are valid data that
            # still need casting (e.g. json.dumps({}) → "{}").
            # Using truthiness would skip the cast for these values,
            # causing psycopg2 "can't adapt type 'dict'" errors.
            #
            # Column-reference expressions are NOT data: the grammar
            # renders them as quoted SQL, so casting (json.dumps etc.)
            # would corrupt them. Leave them untouched.
            if cast and not _is_column_expression(value):
                if value is not None:
                    updates[key] = model.cast_value(key, value)
                else:
                    updates[key] = value
    elif not updates:
        # Do not perform query if there are no updates
        return self

    self._updates = (UpdateQueryExpression(updates),)
    self.set_action("update")
    if dry or self.dry:
        return self

    # Safety: refuse to execute UPDATE without a WHERE clause to
    # prevent accidental mass-mutation.  ``delete()`` has the same
    # guard. If a caller's where-list reduced to empty (e.g. an
    # external id list came back empty and ``where_in`` collapsed
    # to a no-op), this catches the foot-gun before the query runs.
    if not self._wheres:
        raise QueryException(
            "update() without a WHERE clause would modify all rows. "
            "Use ``where_raw('1 = 1')`` to opt in explicitly, "
            "or build the update on a loaded model instance."
        )

    # Column-reference expressions are computed by the database from the
    # CURRENT row — there is no concrete Python value to write back onto
    # the model. Strip them from the dict used to refresh in-memory state
    # so the model never carries a stale ``F`` object as an attribute.
    # (The SQL itself still updates the column server-side.)
    materialized = {
        key: value for key, value in updates.items() if not _is_column_expression(value)
    }

    additional.update(materialized)

    result = self.new_connection().query(self.to_qmark(), self._bindings)
    if model:
        model.fill(materialized)
        self.observe_events(model, "updated")
        model.fill_original(materialized)
        return model
    # Laravel parity: a non-model (table-level) update returns the
    # affected row count. Queue/outbox CAS transitions depend on it —
    # the old ``additional`` dict return was always truthy, so a losing
    # CAS (0 rows matched) still claimed the job.
    return result


def _qb_force_update(self, updates: dict, dry=False):
    return self.update(updates, dry=dry, force=True)


def _qb_set_updates(self, updates: dict, dry=False) -> Self:
    """
    Specifies columns and values to be updated.

    Arguments:
        updates {dictionary} -- A dictionary of columns and values to update.

    Keyword Arguments:
        dry {bool} -- Whether the query should be executed. (default: {False})

    Returns:
        self
    """
    self._updates += (UpdateQueryExpression(updates),)
    return self


def _qb_increment(self, column, value=1, dry=False):
    """
    Increments a column's value.

    Arguments:
        column {string} -- The name of the column.

    Keyword Arguments:
        value {int} -- The value to increment by. (default: {1})

    Returns:
        self
    """
    model = None
    id_key = "id"
    id_value = None

    additional = {}

    if self._model:
        model = self._model
        id_value = self._model.get_primary_key_value()

    if model and model.is_loaded():
        self.where(
            model.get_primary_key(),
            model.get_primary_key_value(),
        )
        additional.update({model.get_primary_key(): model.get_primary_key_value()})

        self.observe_events(model, "updating")

    self._updates += (UpdateQueryExpression(column, value, update_type="increment"),)

    if dry or self.dry:
        return self.get_grammar().compile("update").to_sql()

    self.set_action("update")
    results = self.new_connection().query(self.to_qmark(), self._bindings)
    processed_results = self.get_processor().get_column_value(
        self, column, results, id_key, id_value
    )
    return processed_results


def _qb_decrement(self, column, value=1, dry=False):
    """
    Decrements a column's value.

    Arguments:
        column {string} -- The name of the column.

    Keyword Arguments:
        value {int} -- The value to decrement by. (default: {1})

    Returns:
        self
    """
    model = None
    id_key = "id"
    id_value = None

    additional = {}

    if self._model:
        model = self._model
        id_value = self._model.get_primary_key_value()

    if model and model.is_loaded():
        self.where(
            model.get_primary_key(),
            model.get_primary_key_value(),
        )
        additional.update({model.get_primary_key(): model.get_primary_key_value()})

        self.observe_events(model, "updating")

    self._updates += (UpdateQueryExpression(column, value, update_type="decrement"),)

    if dry or self.dry:
        return self.get_grammar().compile("update").to_sql()

    self.set_action("update")
    result = self.new_connection().query(self.to_qmark(), self._bindings)
    processed_results = self.get_processor().get_column_value(
        self, column, result, id_key, id_value
    )
    return processed_results


def _qb_sum(self, column, dry=False):
    """Get the sum of a column's values.

    Returns:
        The sum in the column's native type — a ``NUMERIC`` column
        sums to ``Decimal`` — or ``None`` when the filter matches
        zero rows (Postgres' ``SUM`` over an empty set returns
        ``NULL``, which psycopg surfaces as Python ``None``).
        Callers that want a numeric zero MUST coerce explicitly,
        and money coerces to ``Decimal``, never ``float``:
        ``qb.sum("amount") or Decimal("0")``.

    Pre-fix the docstring read "or 0 if no results" — incorrect,
    since every call site that didn't ``or 0`` would have hit a
    ``TypeError`` on the empty-table path (``None * 1`` raises).
    The aggregate ``COUNT`` does coerce to ``0`` on empty, but
    ``SUM`` / ``AVG`` / ``MIN`` / ``MAX`` all surface ``None``
    because the SQL semantics differ. The docstring then went on
    to advertise ``float(qb.sum("amount") or 0)`` as canonical,
    which is how the float hop in ``_run_aggregate`` survived
    review — a recommended pattern that loses precision on the
    very column type it was written for.
    """
    return self._run_aggregate("SUM", column, dry)


def _qb_count(self, column=None, dry=False):
    """Get the number of records matching the query.

    Args:
        column: Optional column to count (defaults to *).
        dry: If True, return the builder instead of executing.

    Returns:
        int -- The count of matching records.
    """
    col = column or "*"
    return self._run_aggregate("COUNT", col, dry)


def _qb_max(self, column, dry=False):
    """Get the maximum value of a column.

    Returns:
        The max value, or None if no results.
    """
    return self._run_aggregate("MAX", column, dry)


def _qb_order_by(self, column, direction="ASC") -> Self:
    """
    Specifies a column to order by.

    SECURITY — both ``column`` and ``direction`` are validated.
    SQL grammars splice them in unparameterised, so
    any caller passing a request-supplied value here (sort=...)
    used to be a clean SQL injection sink. Names must look like
    ``foo`` or ``table.column``; direction must be ASC or DESC.
    Anything else raises ``ValueError``.
    """
    for col in column.split(","):
        col = col.strip()
        if not _ORDER_BY_COLUMN_RE.match(col):
            raise InvalidArgumentException(
                f"Invalid order_by column {col!r}. "
                f"Expected ``name`` or ``table.column`` identifier; use "
                f"``order_by_raw`` for expressions."
            )
        dir_str = (direction or "ASC").upper()
        if dir_str not in ("ASC", "DESC"):
            raise InvalidArgumentException(
                f"Invalid order_by direction {direction!r}; expected ASC or DESC"
            )
        self._order_by += (OrderByExpression(col, direction=dir_str),)
    return self


def _qb_order_by_raw(self, query, bindings=None) -> Self:
    """
    Specifies a column to order by.

    Arguments:
        column {string} -- The name of the column.

    Keyword Arguments:
        direction {string} -- Specify either ASC or DESC order. (default: {"ASC"})

    Returns:
        self
    """
    if bindings is None:
        bindings = []
    self._order_by += (OrderByExpression(query, raw=True, bindings=bindings),)
    return self


def _qb_group_by(self, column) -> Self:
    """
    Specifies a column to group by.

    Arguments:
        column {string} -- The name of the column to group by.

    Returns:
        self
    """
    for col in column.split(","):
        self._group_by += (GroupByExpression(column=col),)

    return self


def _qb_group_by_raw(self, query, bindings=None) -> Self:
    """
    Specifies a column to group by.

    Arguments:
        query {string} -- A raw query

    Returns:
        self
    """
    if bindings is None:
        bindings = []
    self._group_by += (GroupByExpression(column=query, raw=True, bindings=bindings),)

    return self


def _qb_aggregate(self, aggregate, column, alias=None):
    """Register an aggregate expression on the builder.

    Arguments:
        aggregate {string} -- The aggregate function (COUNT, SUM, etc.).
        column {string} -- The column expression.
        alias {string} -- Optional alias.
    """
    self._aggregates += (
        AggregateExpression(
            aggregate=aggregate,
            column=column,
            alias=alias,
        ),
    )


def _qb_run_aggregate(self, function, column, dry=False):
    """Execute an aggregate function and return the scalar result.

    Handles stripping ORDER BY (invalid in aggregate-only queries)
    and cleaning up builder state afterward.

    Returns:
        The aggregate result in the driver's NATIVE type — a
        Postgres ``NUMERIC`` column arrives as ``Decimal`` and
        leaves as ``Decimal``. ``None`` when the filter matched
        zero rows, except ``COUNT``, which is ``0`` on empty
        because the SQL semantics differ.

    Pre-fix this branch ran ``float(val)`` for ``AVG`` / ``SUM``
    while ``MIN`` / ``MAX`` / ``COUNT`` on the very same line
    returned the native value — so the same column answered in
    two different types depending on which aggregate you asked
    for. Worse, the float hop destroys money: at cara's own
    ``NUMERIC(17,6)`` ceiling,
    ``float(Decimal('99999999999.999999'))`` is
    ``100000000000.0`` — a penny short of a hundred billion
    rounded up to exactly a hundred billion, silently. Money is
    ``Decimal`` end-to-end; the aggregate does not get to
    downgrade it.
    """
    alias = f"m_{function.lower()}_result"
    self.aggregate(function, f"{column} as {alias}")

    if dry or self.dry:
        return self

    saved_order_by = self._order_by
    self._order_by = ()
    try:
        result = self.new_connection().query(self.to_qmark(), self._bindings, results=1)
    finally:
        self._order_by = saved_order_by

    if result is None:
        return 0 if function == "COUNT" else None

    if isinstance(result, dict):
        val = result.get(alias)
        if val is not None:
            return val
        return 0 if function == "COUNT" else None

    prepared = list(result.values())
    if not prepared:
        return 0 if function == "COUNT" else None
    return prepared[0]
