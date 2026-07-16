"""Tests for the after-commit primitive (Laravel's ``DB::afterCommit``).

Background
~~~~~~~~~~
Before this seam, deferring a job/event "until the surrounding transaction
actually commits" was hand-rolled at every call site (``if not
in_transaction: dispatch() else: stash-and-dispatch-later``). The framework
now exposes a real after-commit hook:

  * ``ConnectionResolver.after_commit(name, cb)`` / ``DB.after_commit(cb)``
  * Callbacks are keyed to the OUTERMOST open transaction and fire exactly
    once, right after the driver-level commit of that transaction succeeds.
  * A rollback of the outermost transaction discards them.
  * With no transaction open, the callback runs immediately.

These tests pin that contract WITHOUT a real database by driving the
resolver with a fake connection that mimics ``transaction_level`` math
(begin → +1, commit/rollback → -1 down to 0), exactly like
``PostgresConnection``.
"""

from __future__ import annotations

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.connections.ConnectionResolver import (
    ConnectionResolver,
    _get_after_commit_registry,
    _get_registry,
)


class _FakeConnection:
    """Minimal stand-in mirroring ``PostgresConnection`` txn-level math."""

    def __init__(self):
        self.transaction_level = 0
        self.open = 1
        self.committed = 0
        self.rolled_back = 0
        self.closed = 0

    def begin(self):
        self.transaction_level += 1
        return self

    def commit(self):
        self.committed += 1
        if self.transaction_level > 0:
            self.transaction_level -= 1
        return self

    def rollback(self):
        self.rolled_back += 1
        if self.transaction_level > 0:
            self.transaction_level -= 1
        return self

    def close_connection(self):
        self.closed += 1


def _resolver_with_fake(conn):
    """A ConnectionResolver whose ``begin_transaction`` pins ``conn``."""
    resolver = ConnectionResolver(database_manager=None)
    # Bypass real connection construction: hand back our fake.
    resolver._create_connection_instance = lambda name: conn  # type: ignore[assignment]
    return resolver


@pytest.fixture(autouse=True)
def _clean_registries():
    """Isolate every test — never leak a pinned conn / pending callback."""
    _get_registry().clear()
    _get_after_commit_registry().clear()
    yield
    _get_registry().clear()
    _get_after_commit_registry().clear()


def test_callback_fires_after_commit():
    conn = _FakeConnection()
    resolver = _resolver_with_fake(conn)
    fired = []

    with resolver.transaction("app"):
        resolver.after_commit("app", lambda: fired.append("committed"))
        # Must NOT have fired yet — still inside the open transaction.
        assert fired == []

    # Outermost commit happened → callback fires exactly once.
    assert fired == ["committed"]
    assert conn.committed == 1


def test_callback_does_not_fire_on_rollback():
    conn = _FakeConnection()
    resolver = _resolver_with_fake(conn)
    fired = []

    with pytest.raises(RuntimeError), resolver.transaction("app"):
        resolver.after_commit("app", lambda: fired.append("nope"))
        raise RuntimeError("boom")  # forces rollback

    # Rolled-back transaction → after-commit callback discarded entirely.
    assert fired == []
    assert conn.rolled_back == 1
    # And the pending-callback list was cleared so it can't fire later.
    assert _get_after_commit_registry().get("app") in (None, [])


def test_callback_runs_immediately_when_no_transaction_open():
    conn = _FakeConnection()
    resolver = _resolver_with_fake(conn)
    fired = []

    # No transaction has been opened on "app" in this context.
    resolver.after_commit("app", lambda: fired.append("now"))

    assert fired == ["now"]


def test_nested_transactions_fire_on_outermost_commit_only():
    conn = _FakeConnection()
    resolver = _resolver_with_fake(conn)
    fired = []

    with resolver.transaction("app"):
        resolver.after_commit("app", lambda: fired.append("outer"))
        with resolver.transaction("app"):
            resolver.after_commit("app", lambda: fired.append("inner"))
            # Inner (savepoint) commit must NOT fire anything yet.
        assert fired == []

    # Only the OUTERMOST commit drains both callbacks, in registration order.
    assert fired == ["outer", "inner"]


def test_db_manager_after_commit_delegates_and_defers():
    """``DatabaseManager.after_commit`` (the DB facade target) defers
    inside a txn and runs immediately outside one."""
    dm = DatabaseManager()
    conn = _FakeConnection()
    # Pin our fake on the manager's resolver so transactions use it.
    resolver = dm._ensure_resolver()
    resolver._create_connection_instance = lambda name: conn  # type: ignore[assignment]

    fired = []
    # Immediate path (no open transaction).
    dm.after_commit(lambda: fired.append("immediate"))
    assert fired == ["immediate"]

    # Deferred path (default connection resolves to "app").
    with dm.transaction():
        dm.after_commit(lambda: fired.append("deferred"))
        assert fired == ["immediate"]
    assert fired == ["immediate", "deferred"]
