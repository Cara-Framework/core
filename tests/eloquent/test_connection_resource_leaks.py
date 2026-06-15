"""Resource-leak regression tests for the eloquent connection layer.

These pin down bugs where a connection (or cursor) is acquired but not
released in an exception / early-exit path. Each test deliberately
triggers the leaky path and asserts the resource has been released.
"""

from __future__ import annotations

import sys
import threading
import types
from unittest.mock import MagicMock

import pytest

import importlib

from cara.eloquent.connections.SQLiteConnection import SQLiteConnection

# Resolve the *module* (not the class re-exported by the package __init__).
PGModule = importlib.import_module("cara.eloquent.connections.PostgresConnection")
PostgresConnection = PGModule.PostgresConnection


# ── SQLiteConnection.select_many: generator abandoned mid-iteration ──


def _seeded_sqlite_connection(rows: int) -> SQLiteConnection:
    """Return an opened in-memory SQLite connection with ``rows`` test rows.

    Built directly rather than going through DatabaseManager so the test
    can inspect ``conn.open`` without the QueryBuilder layer reopening it.
    """
    conn = SQLiteConnection(database=":memory:")
    conn.make_connection()
    cur = conn._connection.cursor()
    cur.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    cur.executemany("INSERT INTO t(id) VALUES (?)", [(i,) for i in range(rows)])
    cur.close()
    return conn


def test_select_many_releases_connection_when_generator_abandoned_early():
    """``select_many`` is a generator; if the caller breaks out of the
    loop after the first batch, the cleanup code in the (missing) finally
    block never runs and ``open`` stays at 1 — slowly leaking SQLite
    connection handles in every paginated-iteration code path."""
    conn = _seeded_sqlite_connection(rows=300)

    gen = conn.select_many("SELECT id FROM t WHERE id >= ?", (0,), 50)
    first_batch = next(gen)
    assert len(first_batch) == 50

    # Caller decides it has enough — abandon the generator.
    gen.close()

    assert conn.open == 0, (
        "select_many leaked: connection was not released after the "
        "generator was closed mid-iteration"
    )


def test_select_many_releases_connection_when_consumer_raises():
    """Same leak, different trigger: an exception in the body of the
    ``for ... in select_many(...)`` loop must still release the connection."""
    conn = _seeded_sqlite_connection(rows=200)

    with pytest.raises(RuntimeError, match="boom"):
        for batch in conn.select_many("SELECT id FROM t WHERE id >= ?", (0,), 25):
            raise RuntimeError("boom")

    assert conn.open == 0, (
        "select_many leaked: connection was not released after the consumer raised"
    )


def test_select_many_releases_connection_after_full_consumption():
    """Belt-and-braces: normal full-iteration must still close the conn.
    Catches regressions where a too-aggressive finally would close the
    connection mid-yield."""
    conn = _seeded_sqlite_connection(rows=100)

    total = 0
    for batch in conn.select_many("SELECT id FROM t WHERE id >= ?", (0,), 30):
        total += len(batch)

    assert total == 100
    assert conn.open == 0


# ── PostgresConnection.create_connection: cursor leak on healthcheck ──


def _install_fake_psycopg2(monkeypatch, connect_factory):
    """Insert a minimal fake ``psycopg2`` module so ``create_connection``
    can ``import psycopg2`` without the real driver installed.

    Returns the fake module so individual tests can inspect / extend it.
    """
    fake = types.ModuleType("psycopg2")
    fake.connect = connect_factory
    fake.OperationalError = type("OperationalError", (Exception,), {})
    monkeypatch.setitem(sys.modules, "psycopg2", fake)
    return fake


def test_create_connection_closes_cursor_when_healthcheck_execute_raises(
    monkeypatch,
):
    """The pool reuse path opens a cursor to issue ``SELECT 1`` as a
    liveness probe. If ``cursor.execute`` raises (broken connection that
    isn't yet reported via ``.closed``), the explicit ``cursor.close()``
    line is jumped over and only the *connection* is closed — leaving
    the cursor dangling on a now-dead connection.

    Under burst load this accumulates and contributes to "too many open
    cursors" / fd pressure on the postgres side."""
    # Stale-but-not-yet-closed connection: SELECT 1 raises, simulating
    # a server-side close or network timeout that psycopg2 hasn't
    # propagated to ``.closed`` yet.
    stale_cursor = MagicMock(name="stale_cursor")
    stale_cursor.execute.side_effect = RuntimeError(
        "server closed the connection unexpectedly"
    )
    stale_conn = MagicMock(name="stale_conn")
    stale_conn.closed = False
    stale_conn.info.transaction_status = 0
    stale_conn.cursor.return_value = stale_cursor

    # Replacement connection for after the stale one is discarded.
    fresh_conn = MagicMock(name="fresh_conn")
    _install_fake_psycopg2(monkeypatch, connect_factory=lambda **kw: fresh_conn)

    # Reset the module-level pool / semaphore so this test is hermetic.
    monkeypatch.setattr(PGModule, "CONNECTION_POOL", [stale_conn])
    monkeypatch.setattr(PGModule, "_pool_initialized", True)
    monkeypatch.setattr(PGModule, "_pool_semaphore", threading.Semaphore(4))

    pc = PostgresConnection(
        host="x",
        database="x",
        user="x",
        port=5432,
        password="x",
        full_details={"connection_pooling_enabled": True},
    )

    returned = pc.create_connection()

    assert returned is fresh_conn, (
        "create_connection should fall through to a fresh connect after "
        "the stale one fails its healthcheck"
    )
    (
        stale_cursor.close.assert_called_once_with(),
        (
            "Cursor opened for the SELECT 1 healthcheck must be closed even "
            "when execute() raises — otherwise it leaks attached to a dead "
            "connection"
        ),
    )
