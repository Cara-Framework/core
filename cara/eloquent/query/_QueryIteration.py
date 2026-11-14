"""Chunked and lazy query iteration for ``QueryBuilder``."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from typing import Any, Self

from cara.eloquent.expressions import QueryExpression, SubGroupExpression
from cara.exceptions import (
    InvalidArgumentException,
    QueryException,
)

from ..casts.DateTimeCast import DateTimeCast
from ._QueryAggregation import _joins_with_or
from ._QuerySafety import ORDER_BY_COLUMN_RE as _ORDER_BY_COLUMN_RE

_logger = logging.getLogger("cara.eloquent.query")
_PLAIN_COLUMN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
QueryBuilder: type


def _bind_query_builder(builder_type: type) -> None:
    global QueryBuilder
    QueryBuilder = builder_type


def _group_wheres(builder) -> None:
    """Wrap OR-joined wheres in one group before another predicate is ANDed on.

    AND binds tighter than OR: ``a OR b AND id > ?`` reads as
    ``a OR (b AND id > ?)``, so a keyset cursor never advanced past the rows
    matching ``a`` and ``chunk_by_id`` looped over them forever.
    """
    if any(_joins_with_or(where) for where in builder._wheres[1:]):
        group = builder.new()
        group._wheres = builder._wheres
        builder._wheres = (QueryExpression(None, "=", SubGroupExpression(group)),)


def _ensure_stable_order(builder) -> None:
    """Order an OFFSET-paged builder by its primary key when it has no order.

    Without ORDER BY the database may return rows in a different order on
    every page, so ``chunk``/``lazy``/``cursor`` skipped some rows and
    yielded others twice. Laravel's ``chunk`` enforces the key order the
    same way. DECISION: a table builder (no model) orders by ``id``, the
    framework-wide key name; a table without one fails loudly instead of
    paging in an undefined order. A keyless model must order explicitly.
    """
    if builder._order_by:
        return
    key = builder._model.get_primary_key() if builder._model else "id"
    if not isinstance(key, str) or not key:
        raise QueryException(
            "chunk()/lazy()/cursor() page with LIMIT/OFFSET and need a stable "
            "order: call order_by() on a model without a single-column key."
        )
    if builder._joins:
        key = f"{builder.get_table_name()}.{key}"
    builder.order_by(key, "asc")


def _validate_key_column(method: str, column: Any) -> None:
    if not isinstance(column, str) or not _ORDER_BY_COLUMN_RE.fullmatch(column):
        raise InvalidArgumentException(
            f"{method}: invalid column name {column!r}. Allowed: "
            f"``[A-Za-z_][A-Za-z0-9_]*`` optionally with a single "
            f"``.<col>`` qualifier (table.column).",
        )


def _keyset_page(self, column: str, last_id: Any, chunk_size: int):
    """One keyset page: the caller's wheres grouped, then ``column > last_id``.

    The caller's own orders are REPLACED by the key order: a ``name`` order
    ahead of ``id`` made ``id > last_id`` page through a different sequence
    than the cursor assumed, revisiting rows forever.
    """
    builder = self.clone()
    if last_id is not None:
        _group_wheres(builder)
        builder.where(column, ">", last_id)
    builder._order_by = ()
    return builder.order_by(column, "asc").limit(chunk_size).get()


def _record_key(record: Any, column: str) -> Any:
    """The key value of a result row, read by the unqualified column name.

    A row exposes ``id``, never ``item.id``: reading the qualified name
    answered ``None`` and the walk stopped after its first chunk.
    """
    name = column.rsplit(".", 1)[-1]
    if isinstance(record, dict):
        return record.get(name)
    return getattr(record, name, None)


def _qb_chunk(self, chunk_size: int, callback: Callable):
    """Process the results in chunks (Laravel-style).

    The callback receives each chunk as a Collection. Return False
    from the callback to stop processing further chunks.

    Args:
        chunk_size: Number of records per chunk.
        callback: Function that receives each chunk Collection.

    Returns:
        bool -- True if all chunks were processed.

    Example:
        def process(chunk):
            for record in chunk:
                record.update({'processed': True})

        Model.active().chunk(200, process)

    Pages are ordered by the primary key unless the builder has its own
    order (see ``_ensure_stable_order``). Use ``chunk_by_id`` when the
    callback changes the rows it filters on.
    """
    page = 1
    while True:
        offset = (page - 1) * chunk_size
        builder = self.clone()
        _ensure_stable_order(builder)
        results = builder.limit(chunk_size).offset(offset).get()

        if not results or (hasattr(results, "is_empty") and results.is_empty()):
            break

        result = callback(results)

        if result is False:
            return False

        count = len(results) if hasattr(results, "__len__") else results.count()
        if count < chunk_size:
            break

        page += 1

    return True


def _qb_upsert(
    self,
    values: list[dict[str, Any]],
    unique_by: list[str],
    update: list[str] | None = None,
    cast: bool = True,
):
    """
    Insert new records or update existing ones (Laravel-style upsert).

    Args:
        values: List of dictionaries with data to insert/update
        unique_by: List of column names that determine uniqueness
        update: List of column names to update on conflict (if None, updates all except unique_by)
        cast: Whether to apply model casts

    Returns:
        Number of affected rows

    Example:
        Receipt.upsert([
            {"receipt_id": "123", "status": "processed", "amount": 100},
            {"receipt_id": "124", "status": "pending", "amount": 200}
        ], unique_by=["receipt_id"], update=["status", "amount", "updated_at"])
    """
    self.set_action("upsert")
    model = None

    if self._model:
        model = self._model

    # Process and validate input data
    processed: list[dict[str, Any]] = []
    for record in values:
        if model:
            # Apply mass assignment protection
            record = model.filter_mass_assignment(record)
            # Apply casts if requested
            if cast:
                record = model.cast_values(record)
        processed.append(dict(record))

    # Stamp timestamps BEFORE the uniform-keys check so a mix of
    # explicitly-timestamped and bare rows still ends up uniform.
    # ``update=[]`` is the explicit insert-if-missing (DO NOTHING)
    # form — no update list to extend there.
    stamp_timestamps = bool(model and getattr(model, "__timestamps__", False))
    if stamp_timestamps:
        # A write value: to the microsecond, like every ORM write.
        timestamp_value = DateTimeCast.sql_instant(model.get_new_date())
        for record in processed:
            if record.get(model.date_created_at) is None:
                record[model.date_created_at] = timestamp_value
            if record.get(model.date_updated_at) is None:
                record[model.date_updated_at] = timestamp_value

    # Every row must cover the same columns. Silently taking row 0's
    # keys (the old behavior) misaligned values under the wrong
    # columns for heterogeneous rows — and filling gaps with NULL
    # would silently overwrite existing data through the
    # ``EXCLUDED.col`` update. Fail loudly instead.
    if processed:
        expected = set(processed[0])
        for i, record in enumerate(processed[1:], start=1):
            if set(record) != expected:
                raise QueryException(
                    "upsert() rows must share the same columns: row 0 has "
                    f"{sorted(expected)}, row {i} has {sorted(record)}."
                )

    # Sorted keys → deterministic column order across rows and runs.
    self._upsert_values = [dict(sorted(record.items())) for record in processed]

    # Store upsert configuration
    self._upsert_unique_by = unique_by

    # If update columns not specified, update all columns except unique_by and timestamps
    if update is None:
        if self._upsert_values:
            all_columns = set(self._upsert_values[0].keys())
            exclude_columns = set(unique_by)

            # Don't auto-update created_at, but do update updated_at
            if model and hasattr(model, "date_created_at"):
                exclude_columns.add(model.date_created_at)

            self._upsert_update = sorted(all_columns - exclude_columns)
        else:
            self._upsert_update = []
    else:
        self._upsert_update = list(update)
        # Laravel parity: updated_at rides along on conflict updates
        # (but an explicit empty list means DO NOTHING — leave it).
        if (
            stamp_timestamps
            and self._upsert_update
            and model.date_updated_at not in self._upsert_update
        ):
            self._upsert_update.append(model.date_updated_at)

    if not self.dry:
        connection = self.new_connection()
        query_result = connection.query(self.to_qmark(), self._bindings)

        # Affected row count: grammars with RETURNING hand back the
        # touched rows (len == inserted + updated); grammars without
        # surface cursor.rowcount as an int.
        if isinstance(query_result, int):
            return query_result
        return len(query_result or [])

    return len(self._upsert_values)


def _qb_bulk_update(
    self,
    records: list[dict[str, Any]],
    key: str = "id",
    update_columns: list[str] | None = None,
):
    """Bulk update multiple records in a single query using PostgreSQL VALUES + UPDATE FROM.

    Args:
        records: List of dicts, each must contain the key column
        key: Column to match records on (default: "id")
        update_columns: Columns to update (if None, updates all except key)

    Returns:
        Number of affected rows

    A record that does not name a column leaves that column as stored; an
    explicit ``None`` writes NULL. The builder's wheres and its ``update``
    global scopes (tenant scope included) bound the write, model casts apply
    and ``updated_at`` is stamped, like every other builder write.

    Example:
        Model.bulk_update([
            {"id": 1, "price": 9.99, "status": "active"},
            {"id": 2, "price": 19.99, "status": "inactive"},
        ], key="id", update_columns=["price", "status"])
    """
    # ROOT CAUSE: this hand-built UPDATE bound a missing key as NULL (a
    # partial record erased the stored value) and never compiled the
    # builder's wheres or scopes, so ``where("tenant_id", 1)`` — and the
    # tenant scope — were dropped and another tenant's row with the same
    # id was rewritten.
    if not records:
        return 0
    if not isinstance(key, str) or not _PLAIN_COLUMN_RE.fullmatch(key):
        raise InvalidArgumentException(f"bulk_update: invalid key column {key!r}.")

    rows = [dict(record) for record in records]
    seen: set[Any] = set()
    for index, row in enumerate(rows):
        if row.get(key) is None:
            raise QueryException(f"bulk_update() record {index} has no {key!r}.")
        if row[key] in seen:
            # Two VALUES rows joining one target row: PostgreSQL applies an
            # arbitrary one. Refuse instead of guessing which value wins.
            raise QueryException(
                f"bulk_update() records repeat {key}={row[key]!r}; "
                "send one record per row."
            )
        seen.add(row[key])

    if update_columns is None:
        columns = [c for c in dict.fromkeys(c for row in rows for c in row) if c != key]
    else:
        columns = [c for c in dict.fromkeys(update_columns) if c != key]
    for column in columns:
        if not isinstance(column, str) or not _PLAIN_COLUMN_RE.fullmatch(column):
            raise InvalidArgumentException(f"bulk_update: invalid column {column!r}.")
    if not columns:
        return 0

    model = self._model
    if model:
        for row in rows:
            row.update(
                model.cast_values(
                    {c: row[c] for c in columns if c in row and row[c] is not None}
                )
            )

    builder = self.clone()
    builder._updates = ()
    builder.set_action("update")
    builder.run_scopes()
    grammar = builder.get_grammar()
    where_sql = grammar.process_wheres(qmark=True, strip_first_where=True).strip()
    where_bindings = list(grammar._bindings)
    target = grammar.process_table(grammar.table)

    bindings: list[Any] = []
    sets: list[str] = []
    partial = [c for c in columns if any(c not in row for row in rows)]
    flags = {c: f"__set_{i}" for i, c in enumerate(partial)}
    for column in columns:
        if column in flags:
            sets.append(
                f'"{column}" = CASE WHEN _bulk."{flags[column]}" '
                f'THEN _bulk."{column}" ELSE {target}."{column}" END'
            )
        else:
            sets.append(f'"{column}" = _bulk."{column}"')
    if (
        model
        and getattr(model, "__timestamps__", False)
        and model.date_updated_at not in columns
    ):
        sets.append(f"\"{model.date_updated_at}\" = '?'")
        bindings.append(DateTimeCast.sql_instant(model.get_new_date()))

    # ROOT CAUSE: a VALUES list types each column from its own literals, and
    # an untyped string or NULL resolves to ``text``. A cast datetime (the
    # cast renders a string), or a ``None`` meant to clear a price, then
    # failed with "column is of type numeric but expression is of type text"
    # or "CASE types numeric and text cannot be matched". A first, never
    # matching row of the target's own (empty) column subqueries gives every
    # VALUES column the stored column's type; its NULL key joins no row.
    tuples = [
        "("
        + ", ".join(
            [
                *(f'(SELECT "{c}" FROM {target} LIMIT 0)' for c in (key, *columns)),
                *("NULL::boolean" for _ in partial),
            ]
        )
        + ")"
    ]
    for row in rows:
        values = [row[key], *(row.get(c) for c in columns), *(c in row for c in partial)]
        bindings.extend(values)
        tuples.append("(" + ", ".join(["'?'"] * len(values)) + ")")
    value_columns = ", ".join(f'"{c}"' for c in (key, *columns, *flags.values()))

    sql = (
        f"UPDATE {target} SET {', '.join(sets)} "
        f"FROM (VALUES {', '.join(tuples)}) AS _bulk({value_columns}) "
        f'WHERE {target}."{key}" = _bulk."{key}"'
    )
    if where_sql:
        sql += f" AND ({where_sql})"
        bindings.extend(where_bindings)

    return self.new_connection().query(sql, tuple(bindings))


def _qb_cursor(self, chunk_size: int = 1000):
    """
    Stream results from the database in memory-bounded chunks.

    Implementation note: ``cursor()`` does NOT open a server-side
    DB cursor — it issues ``LIMIT N OFFSET M`` page queries under
    the hood and yields rows one at a time. The DB connection is
    released between chunks (not held across the whole iteration),
    which keeps the pool free but means the iteration is
    OFFSET-paginated, with two consequences worth knowing:

      * **Cost per chunk grows with offset.** PostgreSQL still
        scans-and-skips ``M`` rows before returning the next ``N``.
        For multi-million-row tables the late chunks dominate the
        total wall clock.
      * **Not stable under concurrent writes.** Inserts/deletes
        mid-iteration can shift the page boundary, causing rows
        to be skipped OR re-yielded across consecutive chunks.

    For large tables that need a stable iteration order or O(1)
    per-chunk cost regardless of position, prefer
    :py:meth:`chunk_by_id` (keyset pagination on a monotonic
    column — ``WHERE id > last_id ORDER BY id LIMIT N``).

    Args:
        chunk_size: Number of records to fetch per page (default: 1000)

    Yields:
        Model: Individual model instances

    Example:
        # Memory-efficient processing of (relatively) static datasets
        for user in User.where('active', True).cursor():
            process_user(user)

        # Process with custom chunk size
        for receipt in Receipt.cursor(chunk_size=500):
            process_receipt(receipt)
    """
    # ROOT CAUSE: each chunk used to be a fresh ``QueryBuilder(model=...)``
    # with the wheres copied over field by field. The constructor reseeds the
    # global scopes from the model, so ``with_trashed()``, ``only_trashed()``
    # and ``without_global_scopes()`` were undone (``only_trashed()`` compiled
    # to ``deleted_at IS NOT NULL AND deleted_at IS NULL``: zero rows), and
    # the caller's own limit/offset were overwritten. A clone carries the
    # builder's exact state, and the pages run inside the caller's window.
    base = 0 if self._offset is False else self._offset
    cap = None if self._limit is False else self._limit
    offset = 0

    while cap is None or offset < cap:
        size = chunk_size if cap is None else min(chunk_size, cap - offset)
        chunk_builder = self.clone()
        _ensure_stable_order(chunk_builder)
        chunk_builder._limit = size
        chunk_builder._offset = base + offset

        chunk_result = (
            chunk_builder.new_connection().query(
                chunk_builder.to_qmark(), chunk_builder._bindings
            )
            or []
        )
        if not chunk_result:
            break

        for record in chunk_result:
            # chunk_builder hydrates so its eager loads apply to each record.
            yield chunk_builder.prepare_result(record, collection=False)

        offset += size
        if len(chunk_result) < size:
            break


def _qb_union(self, query, all=False) -> Self:
    """Append a UNION (or UNION ALL) clause from another QueryBuilder.

    Args:
        query: A QueryBuilder instance whose result set should be unioned.
        all: When True, emit UNION ALL (keeps duplicates).

    Returns:
        self
    """
    if hasattr(query, "get_builder"):
        query = query.get_builder()
    self._unions.append((query, bool(all)))
    return self


def _qb_union_all(self, query):
    """Shortcut for ``union(query, all=True)``."""
    return self.union(query, all=True)


def _qb_chunk_by_id(self, chunk_size: int, callback: Callable, column: str = "id"):
    """Process results in keyset-paginated chunks ordered by ``column``.

    Safer than ``chunk`` for mutating operations because it uses a
    ``WHERE column > last_id`` cursor instead of ``OFFSET`` (which can skip
    rows when records are deleted mid-iteration).
    """
    # Validated at the entry point (``re.fullmatch``, so a trailing ``;``
    # cannot slip past) before the column reaches ``where`` or ``order_by``.
    _validate_key_column("chunk_by_id", column)
    last_id = None
    while True:
        results = _keyset_page(self, column, last_id, chunk_size)

        if not results or (hasattr(results, "is_empty") and results.is_empty()):
            break

        result = callback(results)
        if result is False:
            return False

        last_record = results[-1] if hasattr(results, "__getitem__") else None
        if last_record is None:
            break
        last_id = _record_key(last_record, column)
        if last_id is None:
            break

        count = len(results) if hasattr(results, "__len__") else results.count()
        if count < chunk_size:
            break

    return True


def _qb_lazy(self, chunk_size: int = 1000):
    """Generator interface over ``chunk`` — yields individual records.

    Equivalent of Laravel's ``lazy()``. Memory-efficient streaming.
    """
    page = 1
    while True:
        offset = (page - 1) * chunk_size
        builder = self.clone()
        _ensure_stable_order(builder)
        results = builder.limit(chunk_size).offset(offset).get()
        if not results or (hasattr(results, "is_empty") and results.is_empty()):
            break
        yield from results
        count = len(results) if hasattr(results, "__len__") else results.count()
        if count < chunk_size:
            break
        page += 1


def _qb_lazy_by_id(self, chunk_size: int = 1000, column: str = "id"):
    """Keyset-cursor generator — yields individual records in id order.

    Same safety properties as ``chunk_by_id`` but exposed as a generator.
    """
    _validate_key_column("lazy_by_id", column)
    last_id = None
    while True:
        results = _keyset_page(self, column, last_id, chunk_size)
        if not results or (hasattr(results, "is_empty") and results.is_empty()):
            break
        count = 0
        last_record = None
        for record in results:
            yield record
            last_record = record
            count += 1
        if last_record is None:
            break
        last_id = _record_key(last_record, column)
        if last_id is None or count < chunk_size:
            break
