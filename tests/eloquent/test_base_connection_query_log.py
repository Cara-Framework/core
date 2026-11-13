"""``log_queries`` (``LOG_DB_QUERIES=true``) logs a query instead of crashing it.

``BaseConnection.log`` called ``Log.database(fmt, query, bindings, time)`` —
printf-style arguments — but ``Logger.database(message, level="DEBUG")``
takes one message, so the very first query on a connection with query
logging on raised ``TypeError`` after it had already executed. It also
logged the bindings (they can carry PII; the slow-query warning already
refuses to) and labelled a value in SECONDS as "ms".
"""

from __future__ import annotations

import importlib
from unittest.mock import create_autospec

import pytest

from cara.eloquent.connections.SQLiteConnection import SQLiteConnection
from cara.logging.Logger import Logger

base_connection_module = importlib.import_module(
    "cara.eloquent.connections.BaseConnection"
)


@pytest.fixture
def log(monkeypatch):
    # Autospec carries the REAL ``Logger.database`` signature, so a call the
    # real logger would reject fails here too.
    fake = create_autospec(Logger, instance=True)
    monkeypatch.setattr(base_connection_module, "Log", fake)
    return fake


def _logging_connection():
    # ``full_details`` is the connection's config block as DatabaseManager
    # hands it over; ``log_queries`` is what ``LOG_DB_QUERIES`` sets.
    return SQLiteConnection(database=":memory:", full_details={"log_queries": True})


def test_a_logged_query_runs_and_is_logged_without_its_bindings(log):
    rows = _logging_connection().query(
        "SELECT ? AS secret", ("alice@example.com",), results=1
    )

    assert rows["secret"] == "alice@example.com"
    log.database.assert_called_once()
    (message,), kwargs = log.database.call_args
    assert kwargs == {}
    assert "SELECT ? AS secret" in message
    assert "alice@example.com" not in message


def test_the_logged_time_is_milliseconds(log, monkeypatch):
    ticks = iter([10.0, 10.25])  # a 250 ms statement
    monkeypatch.setattr(base_connection_module, "timer", lambda: next(ticks))

    _logging_connection().query("SELECT 1 AS one", (), results=1)

    (message,), _ = log.database.call_args
    assert "250.00ms" in message
