"""The PostgreSQL side of ``cara.eloquent.ReadSnapshot``: open a session in
the block's snapshot, and begin a transaction that reads it.

A session is a pooled connection (its own pool permit, held by a twin
``PostgresConnection`` for the life of the block) in a ``REPEATABLE READ,
READ ONLY`` transaction that either EXPORTS its snapshot (the block's first
session) or IMPORTS the exported one. Both happen in the one round trip that
opens the transaction. The connection stays in autocommit with the
transaction opened by SQL, so no driver-level ``commit()`` can end it by
accident; the release rolls it back by SQL before the permit is returned.

**Building a relation in a session** (``cara.eloquent.BuiltRelation``,
:func:`build_relations`). The built rows are a temporary table created and
filled inside the session's own transaction, ``ON COMMIT DROP`` — and the
session only ever ends by ``ROLLBACK``, which undoes the creation itself. So
the rows exist for this transaction alone: nothing of them is on the
connection when the pool hands it on, on any exit path (the block's end, a
failed statement, an error while building; a session that cannot roll back
is closed, never pooled).

PostgreSQL creates no table in a ``READ ONLY`` transaction, and a
transaction cannot leave read-only after its first query. So a session that
must build is REOPENED, in the one round trip that builds: ``ROLLBACK`` (a
read-only transaction holds nothing to lose), ``BEGIN … REPEATABLE READ``
importing the SAME snapshot (the holder keeps it importable), the build,
then ``SAVEPOINT`` + ``SET LOCAL transaction_read_only = on``. Every
statement the application runs on that session runs inside that savepoint,
where the database refuses a write (SQLSTATE 25006) and refuses turning
read-only off ("cannot set transaction read-write mode inside a read-only
transaction") — the same guarantee, held by the subtransaction instead of
the ``BEGIN``. A later build of the session rolls back to the savepoint
(which only read), builds at the top level and guards again. The window in
which the session can write is the framework's own build, inside one
``execute``: it either ends guarded or aborts the transaction, and an
aborted session is ended, never used. A session that never builds stays the
``READ ONLY`` transaction it was opened as.

The built table takes the LOOKUP KEYS of the table it stands for, read from
the catalog (the deployed schema is the models' — ``schema:check``), once
per process and table: every valid, whole-table (not partial), plain-column
btree index, without the columns the relation is bound to (one value in
every row: they lead no key), one key per leading column, never UNIQUE (the
rows of a past instant carry today's values in the columns no event states,
so a uniqueness of today need not hold among them). Partial and expression
indexes serve one page's sort on the live table and are not rebuilt.
"""

from __future__ import annotations

import contextlib
from collections.abc import Sequence
from functools import partial
from typing import Any

from ..ReadSnapshot import SNAPSHOT_ID, BuildOrder

_BEGIN = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
_IN_TRANSACTION = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
#: The savepoint every application statement of a building session runs in.
_GUARD = "cara_read_only"
_REOPEN = "ROLLBACK; BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"
_UNGUARD = f"ROLLBACK TO SAVEPOINT {_GUARD}; RELEASE SAVEPOINT {_GUARD}"
_GUARDED = f"SAVEPOINT {_GUARD}; SET LOCAL transaction_read_only = on"
#: The key columns of every valid whole-table plain-column btree index of a
#: table, shortest key first.
_INDEXES = """
    SELECT ARRAY(
        SELECT a.attname::text
        FROM unnest(i.indkey::smallint[]) WITH ORDINALITY AS k(attnum, position)
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
        WHERE k.position <= i.indnkeyatts
        ORDER BY k.position
    ) AS columns
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    JOIN pg_am m ON m.oid = c.relam
    WHERE i.indrelid = to_regclass(%s) AND i.indisvalid AND i.indpred IS NULL
      AND i.indexprs IS NULL AND m.amname = 'btree'
    ORDER BY i.indnkeyatts, c.relname
"""
#: ``(database, table)`` → its indexes' key columns (read once per process).
_INDEXED: dict[tuple[str, str], tuple[tuple[str, ...], ...]] = {}


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def lookup_keys(
    indexes: Sequence[Sequence[str]], bound: Sequence[str]
) -> tuple[tuple[str, ...], ...]:
    """The keys a built relation takes from its table's ``indexes`` (key
    columns each, shortest first): without the ``bound`` columns, one key
    per leading column."""
    kept: dict[str, tuple[str, ...]] = {}
    for columns in indexes:
        key = tuple(column for column in columns if column not in bound)
        if key and key[0] not in kept:
            kept[key[0]] = key
    return tuple(kept.values())


def _indexes_of(connection: Any, cursor: Any, name: str) -> tuple[tuple[str, ...], ...]:
    cached = (str(getattr(connection, "dsn", "")), name)
    indexes = _INDEXED.get(cached)
    if indexes is None:
        cursor.execute(_INDEXES, (_quoted(name),))
        indexes = tuple(tuple(row[0]) for row in cursor.fetchall())
        _INDEXED[cached] = indexes
    return indexes


def _build(table: str, sql: str, keys: Sequence[Sequence[str]]) -> list[str]:
    """The statements that build one relation's rows under ``table``."""
    target = f"pg_temp.{_quoted(table)}"
    steps = [f"CREATE TEMPORARY TABLE {_quoted(table)} ON COMMIT DROP AS {sql}"]
    steps.extend(
        f"CREATE INDEX ON {target} ({', '.join(_quoted(column) for column in key)})"
        for key in keys
    )
    # Every column's statistics, as the table it stands for has them: a
    # page's statement filters on columns that are no key (the bound one
    # first: ``tenant_id = …`` is in every statement), and an equality on a
    # column with no statistics is estimated at 1 row in 200 — measured: a
    # stock page over a built ``product`` analyzed on its keys alone planned
    # nested loops from "2 rows" and ran 16 s instead of 30 ms.
    steps.append(f"ANALYZE {target}")
    return steps


def build_relations(
    connection: Any, exported: str, reopened: bool, orders: Sequence[BuildOrder]
) -> dict[str, str]:
    """Build ``orders`` (``(name, table, sql, bound columns)``) in the
    session on ``connection`` — one round trip, read-only again when it
    returns — and say what a statement reads each relation ``FROM``. Raises
    with the session's transaction aborted: the caller ends the session."""
    if not SNAPSHOT_ID.match(exported):
        raise RuntimeError(f"read_snapshot: not a snapshot id: {exported!r}")
    cursor = connection.cursor()
    try:
        builds: list[str] = []
        for name, table, sql, bound in orders:
            keys = lookup_keys(_indexes_of(connection, cursor, name), bound)
            builds.extend(_build(table, sql, keys))
        opened = (
            _UNGUARD if reopened else f"{_REOPEN}; SET TRANSACTION SNAPSHOT '{exported}'"
        )
        cursor.execute("; ".join([opened, *builds, _GUARDED]))
    finally:
        with contextlib.suppress(Exception):
            cursor.close()
    return {name: f"pg_temp.{_quoted(table)}" for name, table, _sql, _bound in orders}


def _twin(owner: Any) -> Any:
    """A new wrapper on ``owner``'s database and settings (no connection)."""
    twin = type(owner)(
        host=owner.host,
        database=owner.database,
        user=owner.user,
        port=owner.port,
        password=owner.password,
        prefix=owner.prefix,
        options=owner.options,
        full_details=owner.full_details,
        name=owner.name,
    )
    twin.schema = owner.schema
    # Same configured connection, so the twin draws on the owner's pool.
    twin.connection_name = owner.connection_name
    return twin


def open_session(owner: Any, exported: str | None) -> tuple[Any, str | None, Any]:
    """A new session in the block's snapshot: ``(connection, exported id or
    None, release)``. ``exported`` None → this session exports one."""
    holder = _twin(owner)
    connection = holder.pooled_connection()
    holder._connection = connection
    try:
        connection.autocommit = True
        holder.enable_disable_foreign_keys()
        cursor = connection.cursor()
        try:
            if exported is None:
                cursor.execute(f"{_BEGIN}; SELECT pg_export_snapshot()")
                snapshot_id = str(cursor.fetchone()[0])
            else:
                cursor.execute(f"{_BEGIN}; SET TRANSACTION SNAPSHOT %s", (exported,))
                snapshot_id = None
        finally:
            with contextlib.suppress(Exception):
                cursor.close()
    except BaseException:
        _release(holder, connection)
        raise
    return connection, snapshot_id, partial(_release, holder, connection)


def begin_in_snapshot(connection: Any, exported: str) -> None:
    """Open ``connection``'s (driver-managed) transaction in the snapshot."""
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        cursor.execute(f"{_IN_TRANSACTION}; SET TRANSACTION SNAPSHOT %s", (exported,))
    except BaseException:
        with contextlib.suppress(Exception):
            connection.rollback()
            connection.autocommit = True
        raise
    finally:
        with contextlib.suppress(Exception):
            cursor.close()


def _release(holder: Any, connection: Any) -> None:
    """End the session's transaction and hand the connection back."""
    try:
        if not connection.closed:
            cursor = connection.cursor()
            try:
                cursor.execute("ROLLBACK")
            finally:
                cursor.close()
    except Exception:
        # A session that cannot roll back is not handed to the next caller.
        with contextlib.suppress(Exception):
            connection.close()
    finally:
        holder._connection = connection
        holder.close_connection()


__all__ = ["begin_in_snapshot", "build_relations", "lookup_keys", "open_session"]
