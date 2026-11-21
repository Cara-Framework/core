"""The PostgreSQL side of ``cara.eloquent.ReadSnapshot``: open a session in
the block's snapshot, and begin a transaction that reads it.

A session is a pooled connection (its own pool permit, held by a twin
``PostgresConnection`` for the life of the block) in a ``REPEATABLE READ,
READ ONLY`` transaction that either EXPORTS its snapshot (the block's first
session) or IMPORTS the exported one. Both happen in the one round trip that
opens the transaction. The connection stays in autocommit with the
transaction opened by SQL, so no driver-level ``commit()`` can end it by
accident; the release rolls it back by SQL before the permit is returned.
"""

from __future__ import annotations

import contextlib
from functools import partial
from typing import Any

_BEGIN = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
_IN_TRANSACTION = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"


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


__all__ = ["begin_in_snapshot", "open_session"]
