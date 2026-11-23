"""The rolled-back lane is the only place a guarded suite writes.

Synkronus's api suite booted against the developer database and a test that
forgot to fake one write COMMITTED it (autocommit): a real channel's settings
and an authorization counter were rewritten on every run, and outbox rows
signed with the suite's own queue key reached the relay (2026-11-12).
``DatabaseTransactions.guard_outside_lane`` makes every session READ ONLY
outside a ``rolled_back`` block, said with ``SET`` each time (psycopg2 resets
the session default when autocommit goes off and does not send it again when
its attribute already holds the value), and names what it refused.
"""

from __future__ import annotations

import importlib
import logging

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.connections.ConnectionResolver import _get_registry
from cara.testing import DatabaseTransactions

module = importlib.import_module("cara.testing.DatabaseTransactions")


class _Conn:
    """A connection that only counts levels, as the resolver drives one."""

    def __init__(self) -> None:
        self.transaction_level = 0
        self.calls: list[str] = []

    def begin(self):
        self.transaction_level += 1
        self.calls.append("begin")
        return self

    def commit(self):
        self.transaction_level = max(0, self.transaction_level - 1)
        self.calls.append("commit")
        return self

    def rollback(self):
        self.transaction_level = max(0, self.transaction_level - 1)
        self.calls.append("rollback")
        return self

    def close_connection(self):
        self.calls.append("close")


@pytest.fixture
def lane(monkeypatch):
    """The REAL ``DatabaseManager`` over one counting connection: a stub of
    ``DB`` would keep answering for calls the manager no longer makes."""
    _get_registry().clear()
    connection = _Conn()
    manager = DatabaseManager("app", {"app": {"driver": "sqlite"}})
    manager._ensure_resolver()._create_connection_instance = lambda name: connection
    monkeypatch.setattr(module, "DB", manager)
    yield manager, connection
    _get_registry().clear()


class _Cursor:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql):
        self.log.append(sql)


class _Raw:
    def __init__(self, *, autocommit=True):
        self.closed = False
        self.autocommit = autocommit
        self.log: list[str] = []

    def cursor(self):
        return _Cursor(self.log)


def test_outside_the_lane_the_session_is_read_only_and_inside_it_writable(monkeypatch):
    raw = _Raw()
    connection = type("C", (), {"_connection": raw})()

    module._session_mode(connection)
    assert raw.log == ["SET default_transaction_read_only = on"]

    monkeypatch.setattr(DatabaseTransactions, "_lane_depth", 1)
    module._session_mode(connection)
    assert raw.log[-1] == "SET default_transaction_read_only = off"


def test_inside_a_transaction_the_session_is_left_alone():
    raw = _Raw(autocommit=False)
    module._session_mode(type("C", (), {"_connection": raw})())
    assert raw.log == []


def test_the_lane_opens_and_closes_its_depth_even_when_the_test_fails(lane):
    manager, connection = lane
    depth = DatabaseTransactions._lane_depth
    try:
        with DatabaseTransactions.rolled_back("app"):
            assert DatabaseTransactions._lane_depth == depth + 1
            assert manager.transaction_level("app") == 1
            raise RuntimeError("the test failed")
    except RuntimeError:
        pass
    assert DatabaseTransactions._lane_depth == depth
    assert connection.calls == ["begin", "rollback", "close"]
    assert "app" not in _get_registry()


def test_a_transaction_the_test_left_open_is_rolled_back_with_the_lane(lane):
    """One ``DB.rollback`` unwinds one level. Code under test that opened a
    transaction and never closed it (an exception between ``begin`` and
    ``commit``) left the lane's OUTER transaction open and pinned: every later
    test ran as a savepoint inside it."""
    manager, connection = lane
    with DatabaseTransactions.rolled_back("app"):
        manager.begin_transaction("app")
        manager.begin_transaction("app")
        assert manager.transaction_level("app") == 3
    assert manager.transaction_level("app") == 0
    assert "app" not in _get_registry()
    assert connection.calls.count("rollback") == 3


def test_a_nested_lane_returns_to_the_level_it_was_opened_at(lane):
    manager, _connection = lane
    with DatabaseTransactions.rolled_back("app"):
        with DatabaseTransactions.rolled_back("app"):
            manager.begin_transaction("app")  # left open by the inner body
            assert manager.transaction_level("app") == 3
        # the outer lane's own transaction is not the inner lane's to end
        assert manager.transaction_level("app") == 1
    assert manager.transaction_level("app") == 0


def test_a_body_that_drained_the_lane_is_reported_not_raised(lane, caplog):
    manager, connection = lane
    with (
        caplog.at_level(logging.WARNING, logger="cara.testing.database"),
        DatabaseTransactions.rolled_back("app"),
    ):
        manager.commit("app")  # past the lane's outer BEGIN: the write persisted
    assert "drained its outer transaction" in caplog.text
    assert "rollback" not in connection.calls


def test_rows_written_before_an_unclosed_transaction_do_not_reach_the_next_lane(
    tmp_path, monkeypatch
):
    """The leak end to end, on a real SQLite file: the first lane's row was
    still visible inside the second lane."""
    _get_registry().clear()
    manager = DatabaseManager(
        "app", {"app": {"driver": "sqlite", "database": str(tmp_path / "lane.sqlite3")}}
    )
    monkeypatch.setattr(module, "DB", manager)
    manager.statement("CREATE TABLE lane_rows (id INTEGER)")
    try:
        with DatabaseTransactions.rolled_back("app"):
            manager.statement("INSERT INTO lane_rows VALUES (1)")
            manager.begin_transaction("app")  # never committed, never rolled back
            manager.statement("INSERT INTO lane_rows VALUES (2)")
        assert manager.transaction_level("app") == 0
        with DatabaseTransactions.rolled_back("app"):
            assert manager.select("SELECT id FROM lane_rows") == []
    finally:
        _get_registry().clear()


def test_a_refused_write_is_named_once_then_forgotten():
    class _ReadOnly(Exception):
        pgcode = "25006"

    wrapped = RuntimeError("cannot execute UPDATE in a read-only transaction")
    wrapped.__cause__ = _ReadOnly()
    assert module._read_only_refusal(wrapped) is True
    assert module._read_only_refusal(RuntimeError("syntax error")) is False

    DatabaseTransactions._refused.append("UPDATE channel SET settings = …")
    assert DatabaseTransactions.refused_writes() == ["UPDATE channel SET settings = …"]
    assert DatabaseTransactions.refused_writes() == []
