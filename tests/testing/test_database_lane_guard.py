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

from cara.testing import DatabaseTransactions

module = importlib.import_module("cara.testing.DatabaseTransactions")


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


def test_the_lane_opens_and_closes_its_depth_even_when_the_test_fails(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(
        module,
        "DB",
        type(
            "DB",
            (),
            {
                "begin_transaction": staticmethod(lambda name: calls.append("begin")),
                "rollback": staticmethod(lambda name: calls.append("rollback")),
            },
        ),
    )
    depth = DatabaseTransactions._lane_depth
    try:
        with DatabaseTransactions.rolled_back("app"):
            assert DatabaseTransactions._lane_depth == depth + 1
            raise RuntimeError("the test failed")
    except RuntimeError:
        pass
    assert DatabaseTransactions._lane_depth == depth
    assert calls == ["begin", "rollback"]


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
