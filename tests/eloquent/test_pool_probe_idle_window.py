"""A pooled connection is not re-probed right after it answered a statement.

Outside a transaction every statement checks a connection out of the pool and
back in. ``pooled_connection`` sent ``SELECT 1`` on EVERY checkout, so every
such statement cost two round trips — the probe, then the query — on all four
deployables (pooling is on everywhere). A connection that was checked in
within the idle window (``_POOL_PROBE_IDLE_SECONDS``) has just completed a
statement and is handed out as it is; one that sat idle longer, or whose
check-in time is unknown, is still probed first.

Driven through a fake psycopg2 that records every statement it is given.
"""

from __future__ import annotations

import importlib
import sys
import types

import pytest

from cara.eloquent.connections import PostgresPool

from ._fixtures import install_fake_psycopg2, pool_of

PGModule = importlib.import_module("cara.eloquent.connections.PostgresConnection")
PostgresConnection = PGModule.PostgresConnection


class _Info:
    transaction_status = 0


class _Cursor:
    description = None
    rowcount = 1

    def __init__(self, log: list[str]):
        self._log = log

    def execute(self, query, bindings=None):
        self._log.append(query)

    def close(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class _Driver:
    """One fake server session: records what is executed on it."""

    def __init__(self):
        self.closed = False
        self.autocommit = True
        self.info = _Info()
        self.log: list[str] = []

    def cursor(self, cursor_factory=None):
        return _Cursor(self.log)

    def rollback(self):
        return None

    def close(self):
        self.closed = True


@pytest.fixture
def sessions(monkeypatch):
    opened: list[_Driver] = []

    def connect(**_kwargs):
        opened.append(_Driver())
        return opened[-1]

    install_fake_psycopg2(monkeypatch, connect)
    monkeypatch.setitem(
        sys.modules, "psycopg2.extras", types.SimpleNamespace(RealDictCursor=object)
    )
    PostgresPool.reset_all(close=False)
    yield opened
    PostgresPool.reset_all(close=False)


@pytest.fixture
def clock(monkeypatch):
    now = [1000.0]
    # ``time`` is one module: the pool stamps check-ins and the wrapper
    # reads them on this same clock.
    monkeypatch.setattr(PGModule.time, "monotonic", lambda: now[0])
    return now


def _wrapper(**details) -> PostgresConnection:
    return PostgresConnection(
        host="x",
        database="x",
        user="x",
        port=5432,
        password="x",
        full_details={
            "connection_pooling_enabled": True,
            "connection_pooling_max_size": 4,
            **details,
        },
    )


def _run(statement: str, **details) -> None:
    _wrapper(**details).query(statement, ())


def test_back_to_back_statements_send_no_probe(sessions, clock):
    for index in range(5):
        _run(f"UPDATE t SET n = {index}")
        clock[0] += 0.05

    assert len(sessions) == 1
    assert sessions[0].log == [f"UPDATE t SET n = {index}" for index in range(5)]


def test_connection_idle_past_the_window_is_probed(sessions, clock):
    _run("UPDATE t SET n = 1")
    clock[0] += PGModule._POOL_PROBE_IDLE_SECONDS + 0.01
    _run("UPDATE t SET n = 2")

    assert sessions[0].log == ["UPDATE t SET n = 1", "SELECT 1", "UPDATE t SET n = 2"]


def test_connection_with_no_check_in_time_is_probed(sessions, clock):
    warm = _Driver()
    pool_of(_wrapper()).warm(lambda: warm, 1)  # pre-warmed: never checked in

    _run("UPDATE t SET n = 1")

    assert warm.log == ["SELECT 1", "UPDATE t SET n = 1"]
    assert sessions == []


def test_a_zero_window_probes_every_checkout(sessions, clock):
    for index in range(3):
        _run(f"UPDATE t SET n = {index}", connection_pooling_probe_idle_seconds=0)

    assert sessions[0].log == [
        "UPDATE t SET n = 0",
        "SELECT 1",
        "UPDATE t SET n = 1",
        "SELECT 1",
        "UPDATE t SET n = 2",
    ]


def test_a_dead_unprobed_connection_is_dropped_not_pooled(sessions, clock):
    _run("UPDATE t SET n = 1")
    first = sessions[0]

    def die(query, bindings=None):
        first.closed = True
        raise RuntimeError("server closed the connection unexpectedly")

    first.cursor = lambda cursor_factory=None: type(
        "_Dying", (_Cursor,), {"execute": staticmethod(die)}
    )(first.log)

    with pytest.raises(PGModule.QueryException):
        _run("UPDATE t SET n = 2")

    # The dead session never went back to the pool; the next statement
    # opens a new one, and the permit of the failed one was returned.
    pool = pool_of(_wrapper())
    assert pool.stats()["idle"] == 0
    _run("UPDATE t SET n = 3")
    assert len(sessions) == 2
    assert sessions[1].log == ["UPDATE t SET n = 3"]
    assert pool.stats()["in_use"] == 0
