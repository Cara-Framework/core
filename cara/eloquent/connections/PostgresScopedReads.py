"""How a ``PostgresConnection`` reads inside a read scope.

The part of the connection that ``cara.eloquent.ReadScope`` and
``cara.eloquent.ReadSnapshot`` decide: which session a statement runs on
(a session leased from the block's snapshot, handed back when the wrapper
closes), how a transaction begins inside the block, and the one chokepoint
every statement passes — read-only at the database, over the scope's
relations, the built ones built once in the leased session
(``PostgresReadSnapshot.build_relations``).

The wrapper's two fields are set by ``PostgresConnection.__init__``:
``_read_session`` (the block whose session it leases, never pooled by it)
and ``_snapshot_transaction`` (its own transaction was opened in that
snapshot).
"""

from __future__ import annotations

import contextlib
from functools import partial
from typing import Any, Self

from ..ReadScope import current_read_scope, read_only_statement
from .PostgresReadSnapshot import begin_in_snapshot, build_relations, open_session


def scope_snapshot() -> Any:
    """The ``read_snapshot()`` block this context reads in, or None."""
    scope = current_read_scope()
    return scope.snapshot if scope is not None else None


class PostgresScopedReads:
    """Statements, sessions and transactions of a connection inside a read
    scope (mixed into ``PostgresConnection``, ahead of its base)."""

    def _lease_read_session(self) -> Any:
        """Inside ``read_snapshot()``: a session of the block's snapshot,
        leased until ``close_connection``. None outside a block."""
        snapshot = scope_snapshot()
        if snapshot is None:
            return None
        connection = snapshot.session(partial(open_session, self))
        self._read_session = snapshot
        return connection

    def _return_read_session(self) -> None:
        """Hand the leased session back to its block, which ends it."""
        snapshot, connection = self._read_session, self._connection
        self._connection = None
        self._read_session = None
        snapshot.returned(connection)

    def _begin_in_snapshot(self, snapshot: Any) -> Self:
        """A transaction inside ``read_snapshot()``: its own pooled
        connection (the leased sessions stay the plain reads'), opened
        read-only in the block's snapshot — nesting and savepoints as ever.
        A relation the scope builds once is read inline here: the built rows
        are a session's own."""
        exported = snapshot.snapshot_id(partial(open_session, self))
        if self._read_session is not None:
            self._return_read_session()
        if not self._connection or self._connection.closed:
            self._connection = self.pooled_connection()
        try:
            begin_in_snapshot(self._connection, exported)
        except BaseException:
            with contextlib.suppress(Exception):
                self.close_connection()
            raise
        self._snapshot_transaction = True
        self.transaction_level += 1
        return self

    def statement(self, query: Any, bindings: Any = ()) -> Any:
        """Every statement of this connection passes here (``DB.select`` and
        ``DB.statement`` call it directly, the query builder through
        ``query``). Inside a read scope (``cara.eloquent.ReadScope``) it runs
        read-only at the database, over the scope's relations; its result is
        already buffered on the cursor when the read-only transaction ends.

        Inside ``read_snapshot()`` the statement runs as it is, in one round
        trip: the leased session (or this wrapper's transaction) is already
        a read-only transaction in the block's snapshot. A relation the scope
        builds once (``BuiltRelation``) is built in the session by the first
        statement that names it and read from there by the later ones. A
        failure aborts that session only (what it built goes with it); the
        next statement leases another or imports afresh."""
        scope = current_read_scope()
        if scope is None:
            return super().statement(query, bindings)
        # Refusals are raised here, before any session does any work.
        sql = scope.statement(query)
        snapshot = scope.snapshot
        if snapshot is not None and self._snapshot_transaction:
            return super().statement(sql, bindings)
        if snapshot is not None and self._read_session is snapshot:
            try:
                wanted = scope.built_in(query)
                if wanted:
                    sql = scope.statement(
                        query,
                        snapshot.relations(self._connection, wanted, build_relations),
                    )
                return super().statement(sql, bindings)
            except BaseException:
                snapshot.failed(self._connection)
                raise
        with read_only_statement(self._connection):
            return super().statement(sql, bindings)


__all__ = ["PostgresScopedReads", "scope_snapshot"]
