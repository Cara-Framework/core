"""Chunked and lazy query iteration for ``QueryBuilder``."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Self

from cara.exceptions import (
    InvalidArgumentException,
    QueryException,
)

from ..casts.DateTimeCast import DateTimeCast
from ._QuerySafety import ORDER_BY_COLUMN_RE as _ORDER_BY_COLUMN_RE

_logger = logging.getLogger("cara.eloquent.query")
QueryBuilder: type


def _bind_query_builder(builder_type: type) -> None:
    global QueryBuilder
    QueryBuilder = builder_type


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
    """
    page = 1
    while True:
        offset = (page - 1) * chunk_size
        builder = self.clone()
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

    Example:
        Model.bulk_update([
            {"id": 1, "price": 9.99, "status": "active"},
            {"id": 2, "price": 19.99, "status": "inactive"},
        ], key="id", update_columns=["price", "status"])
    """
    if not records:
        return 0

    # Determine columns to update
    if update_columns is None:
        update_columns = [k for k in records[0] if k != key]

    if not update_columns:
        return 0

    # Build VALUES clause
    all_columns = [key] + update_columns
    placeholders = []
    bindings = []
    for record in records:
        row_placeholders = []
        for col in all_columns:
            bindings.append(record.get(col))
            row_placeholders.append("%s")
        placeholders.append(f"({', '.join(row_placeholders)})")

    values_clause = ", ".join(placeholders)
    col_defs = ", ".join(f'"{c}"' for c in all_columns)
    set_clause = ", ".join(f'"{c}" = _bulk."{c}"' for c in update_columns)
    table = self._table.name if hasattr(self._table, "name") else str(self._table)

    sql = f'''
        UPDATE "{table}" SET {set_clause}
        FROM (VALUES {values_clause}) AS _bulk({col_defs})
        WHERE "{table}"."{key}" = _bulk."{key}"
    '''

    connection = self.new_connection()
    return connection.query(sql, tuple(bindings))


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
    # Upfront column validation — same ``_ORDER_BY_COLUMN_RE``
    # gate that ``order_by`` applies, but enforced HERE so a
    # caller passing a bad column name fails fast at the entry
    # point instead of after the WHERE clause has already been
    # queued onto a clone. Pre-fix the validation lived only
    # inside ``order_by`` (line ~3735), which meant ``where``
    # (line ~3733) accepted the column without checking; an
    # attacker-shaped ``"id; DROP TABLE x"`` never actually
    # reached the DB (order_by raised first, killing the query)
    # but the failure surfaced in the wrong place and the WHERE
    # quirk was a foot-gun waiting for the next refactor to
    # swap the order. ``re.fullmatch`` (not ``match``) so a
    # trailing ``;`` doesn't slip past.
    if not isinstance(column, str) or not _ORDER_BY_COLUMN_RE.fullmatch(column):
        raise InvalidArgumentException(
            f"chunk_by_id: invalid column name {column!r}. Allowed: "
            f"``[A-Za-z_][A-Za-z0-9_]*`` optionally with a single "
            f"``.<col>`` qualifier (table.column).",
        )
    last_id = None
    while True:
        builder = self.clone()
        if last_id is not None:
            builder = builder.where(column, ">", last_id)
        results = builder.order_by(column, "asc").limit(chunk_size).get()

        if not results or (hasattr(results, "is_empty") and results.is_empty()):
            break

        result = callback(results)
        if result is False:
            return False

        last_record = results[-1] if hasattr(results, "__getitem__") else None
        if last_record is None:
            break
        last_id = (
            getattr(last_record, column, None)
            if not isinstance(last_record, dict)
            else last_record.get(column)
        )
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
    last_id = None
    while True:
        builder = self.clone()
        if last_id is not None:
            builder = builder.where(column, ">", last_id)
        results = builder.order_by(column, "asc").limit(chunk_size).get()
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
        last_id = (
            getattr(last_record, column, None)
            if not isinstance(last_record, dict)
            else last_record.get(column)
        )
        if last_id is None or count < chunk_size:
            break
