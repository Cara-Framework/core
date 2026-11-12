"""Per-test database isolation — wrap a test in a transaction that is ALWAYS
rolled back, so live-DB integration tests leave no trace.

Generic framework primitive (no app/domain assumptions): an app wires a pytest
fixture around :meth:`DatabaseTransactions.rolled_back`. Every query issued
inside the block joins the connection pinned in Cara's per-context transaction
registry (``ConnectionResolver._ACTIVE_CONNECTIONS``), so writes are visible
WITHIN the test but never durably committed — teardown rolls the outer
transaction back, discarding everything (including nested ``atomic()``
SAVEPOINTs).

Contract: the test body must not COMMIT past the fixture's outer ``BEGIN``
(i.e. don't drain the transaction to level 0). Repository ``atomic()`` blocks
are safe — under an open outer transaction they become SAVEPOINTs and unwind
symmetrically, so their "commit" only RELEASEs a savepoint and never persists.

OUTSIDE the block a test may still reach the database — an app's suite boots
against its developer database, and a test that forgot to fake one call reads
it. :meth:`DatabaseTransactions.guard_outside_lane` makes that reach READ ONLY:
every Postgres session opened while no ``rolled_back`` block is open refuses
writes, and :meth:`DatabaseTransactions.refused_writes` names the ones it
refused so the suite can fail the test. Without it a test's write COMMITTED to
the developer's database (autocommit): Synkronus's api suite rewrote a real
channel's settings and an authorization counter on every run, and an outbox
row signed with the suite's own queue key reached the relay, which (correctly)
quarantined it as an unknown key (2026-11-12).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from cara.eloquent.connections import PostgresConnection
from cara.facades import DB

_logger = logging.getLogger("cara.testing.database")

#: PostgreSQL's ``read_only_sql_transaction`` — a write in a READ ONLY session.
_READ_ONLY_SQLSTATE = "25006"


class DatabaseTransactions:
    """Transactional test-isolation helper. See the module docstring."""

    #: Open ``rolled_back`` blocks in this process — the only place a guarded
    #: suite may write.
    _lane_depth = 0
    #: Whether ``guard_outside_lane`` wrapped the Postgres connection.
    _guarded = False
    #: Writes the guard refused, drained by ``refused_writes``.
    _refused: list[str] = []

    @staticmethod
    @contextmanager
    def rolled_back(connection: str = "app") -> Iterator[None]:
        """Open an outer transaction, yield, and ALWAYS roll it back.

        Opening the transaction pins the connection in the context registry so
        every query in the block rides it; the teardown rollback discards all
        writes. Safe to use against a shared dev database — nothing persists.
        """
        DatabaseTransactions._lane_depth += 1
        try:
            DB.begin_transaction(connection)
            try:
                yield
            finally:
                try:
                    DB.rollback(connection)
                except Exception:  # log misuse, never mask the test result
                    # No active transaction at teardown means the test body
                    # committed past the fixture's outer BEGIN, so writes may
                    # have PERSISTED (isolation lost for this test). Surface
                    # it loudly but do not raise — a teardown error must not
                    # swallow a test assertion/exception already propagating.
                    _logger.warning(
                        "DatabaseTransactions.rolled_back: no active transaction "
                        "on %r at teardown — the test drained its outer "
                        "transaction, so writes may have persisted (isolation "
                        "lost).",
                        connection,
                        exc_info=True,
                    )
        finally:
            DatabaseTransactions._lane_depth -= 1

    @staticmethod
    def guard_outside_lane() -> None:
        """Make every Postgres session READ ONLY while no ``rolled_back``
        block is open (idempotent; call once from the suite's conftest).

        The session's own ``default_transaction_read_only`` does the refusing,
        so a CTE, a function or a ``RETURNING`` write is caught as surely as a
        bare ``UPDATE``. It is applied where a session is (re)used outside a
        transaction — after the connection is made, before an outer ``BEGIN``
        and before each query — because psycopg2 sends it only in autocommit
        mode, and a pooled session keeps the last value it was given.
        """
        if DatabaseTransactions._guarded:
            return
        make_connection = PostgresConnection.make_connection
        begin = PostgresConnection.begin
        query = PostgresConnection.query

        def guarded_make_connection(self):
            result = make_connection(self)
            _session_mode(self)
            return result

        def guarded_begin(self):
            if getattr(self, "transaction_level", 0) == 0:
                _session_mode(self)
            return begin(self)

        def guarded_query(self, sql, bindings=(), results="*"):
            _session_mode(self)
            try:
                return query(self, sql, bindings, results)
            except Exception as exc:
                if DatabaseTransactions._lane_depth == 0 and _read_only_refusal(exc):
                    text = sql if isinstance(sql, str) else "; ".join(map(str, sql))
                    DatabaseTransactions._refused.append(" ".join(text.split())[:200])
                raise

        PostgresConnection.make_connection = guarded_make_connection
        PostgresConnection.begin = guarded_begin
        PostgresConnection.query = guarded_query
        DatabaseTransactions._guarded = True

    @staticmethod
    def refused_writes() -> list[str]:
        """The writes the guard refused since the last call (and forget them)
        — a suite fails the test that made them, even when the code under
        test swallowed the database's refusal."""
        refused = list(DatabaseTransactions._refused)
        DatabaseTransactions._refused.clear()
        return refused


def _session_mode(connection: Any) -> None:
    """Set the raw session READ ONLY outside the lane, writable inside it —
    only between transactions, where the session default is what the next
    statement or ``BEGIN`` inherits.

    Said with ``SET`` every time, not psycopg2's ``readonly`` attribute: the
    driver resets the session's default when autocommit goes off (every
    ``begin``, every pooled hand-back) and does not send it again when the
    attribute already holds the value, so a reused session read ``on`` in
    Python and ``off`` on the server.
    """
    raw = getattr(connection, "_connection", None)
    if (
        raw is None
        or getattr(raw, "closed", True)
        or not getattr(raw, "autocommit", False)
    ):
        return
    mode = "on" if DatabaseTransactions._lane_depth == 0 else "off"
    with raw.cursor() as cursor:
        cursor.execute(f"SET default_transaction_read_only = {mode}")


def _read_only_refusal(exc: BaseException | None) -> bool:
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if getattr(exc, "pgcode", None) == _READ_ONLY_SQLSTATE:
            return True
        exc = exc.__cause__ or exc.__context__
    return False
