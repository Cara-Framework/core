"""A finished SQLite transaction closes its connection without a leak warning.

Live defect: the resolver hands every finished transaction's wrapper to
``close_connection()`` (``ConnectionResolver._safe_close``). SQLite had no
such method, so each commit and rollback ended in a swallowed
``AttributeError`` logged as "Connection close failed (potential pool
leak)": a false alarm on every transaction of every SQLite connection, which
buried the real leak warnings of the PostgreSQL pool under noise.
``SQLiteConnection.close_connection`` now closes the handle.

Executed against a file-backed SQLite database.
"""

from __future__ import annotations

import logging
import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.connections import SQLiteConnection


@pytest.fixture
def manager(tmp_path):
    path = str(tmp_path / "close.sqlite")
    with sqlite3.connect(path) as seed:
        seed.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    return DatabaseManager("app", {"app": {"driver": "sqlite", "database": path}})


def _leak_warnings(caplog) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if "potential pool leak" in record.getMessage()
    ]


def test_close_connection_closes_the_handle():
    connection = SQLiteConnection(database=":memory:")
    connection.make_connection()
    handle = connection._connection

    connection.close_connection()

    assert connection.open == 0
    with pytest.raises(sqlite3.ProgrammingError):
        handle.execute("SELECT 1")


def test_close_connection_on_a_never_opened_wrapper_is_harmless():
    connection = SQLiteConnection(database=":memory:")

    connection.close_connection()

    assert connection.open == 0


def test_a_committed_transaction_logs_no_leak_warning(manager, caplog):
    caplog.set_level(logging.WARNING, logger="cara.database.pool")

    with manager.transaction("app"):
        manager.statement("INSERT INTO t (id) VALUES (1)")

    assert _leak_warnings(caplog) == []
    assert manager.select("SELECT id FROM t") == [{"id": 1}]


def test_a_rolled_back_transaction_logs_no_leak_warning(manager, caplog):
    caplog.set_level(logging.WARNING, logger="cara.database.pool")

    with pytest.raises(RuntimeError, match="abort"), manager.transaction("app"):
        manager.statement("INSERT INTO t (id) VALUES (2)")
        raise RuntimeError("abort")

    assert _leak_warnings(caplog) == []
    assert manager.select("SELECT id FROM t") == []
