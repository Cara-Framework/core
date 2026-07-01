"""Deadlock / serialization retry for ``atomic(attempts=N)``.

Background
~~~~~~~~~~
Postgres aborts one party of a deadlock with SQLSTATE ``40P01`` and aborts
a ``SERIALIZABLE`` / ``REPEATABLE READ`` transaction that loses a race with
``40001``. Both are transient — the documented remedy is "roll back and
re-run the whole transaction". ``atomic(attempts=N)`` re-invokes the closure
on either code, but ONLY when the call OWNS the outer transaction: a nested
savepoint caller cannot roll the outer transaction back, so it must re-raise
and let the outermost ``atomic`` do the retrying.

These tests drive the retry contract WITHOUT a real database by pinning a
fake connection (mirroring ``PostgresConnection``'s ``transaction_level``
math) on the singleton ``DatabaseManager``'s resolver — the same technique
``test_after_commit.py`` uses.
"""

from __future__ import annotations

import pytest

from cara.eloquent.connections.ConnectionResolver import (
    _get_after_commit_registry,
    _get_registry,
)
from cara.eloquent.DatabaseManager import DatabaseManager
from cara.eloquent.Transactions import Atomic, atomic


class _FakeConnection:
    """Stand-in mirroring ``PostgresConnection`` transaction-level math."""

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


class _Deadlock(Exception):
    """Simulated Postgres deadlock — carries the retriable SQLSTATE."""

    pgcode = "40P01"


class _Serialization(Exception):
    pgcode = "40001"


class _NonRetriable(Exception):
    """A different DB error (e.g. a check-constraint violation)."""

    pgcode = "23514"


@pytest.fixture(autouse=True)
def _fake_db(monkeypatch):
    """Pin a fresh fake connection on the singleton DB manager's resolver.

    Clears the per-context transaction registry around every test so a
    pinned connection / pending callback never leaks between cases. Returns
    the fake so a test can assert on commit/rollback counts. ``time.sleep``
    is stubbed so the retry backoff doesn't slow the suite.
    """
    _get_registry().clear()
    _get_after_commit_registry().clear()

    conn = _FakeConnection()
    dm = DatabaseManager.get_instance()
    resolver = dm._ensure_resolver()
    monkeypatch.setattr(
        resolver, "_create_connection_instance", lambda name: conn, raising=False
    )
    # Don't actually sleep through the retry backoff in tests.
    monkeypatch.setattr("cara.eloquent.Transactions.time.sleep", lambda _s: None)

    yield conn

    _get_registry().clear()
    _get_after_commit_registry().clear()


# ── core scenario: raise 40P01 twice, then succeed ──────────────────────


def test_retries_on_deadlock_twice_then_commits(_fake_db):
    """Closure raises 40P01 on the first two attempts, succeeds on the
    third. With ``attempts=3`` it runs exactly 3× and the final attempt
    commits its (owned) transaction."""
    calls = {"n": 0}

    @atomic(attempts=3)
    def work():
        calls["n"] += 1
        if calls["n"] < 3:
            raise _Deadlock("deadlock detected")
        return "ok"

    result = work()

    assert result == "ok"
    assert calls["n"] == 3, "closure must run once per attempt up to success"
    # Two failed attempts rolled back, the third committed.
    assert _fake_db.rolled_back == 2
    assert _fake_db.committed == 1


def test_retries_on_serialization_failure(_fake_db):
    """40001 (serialization_failure) is retriable just like 40P01."""
    calls = {"n": 0}

    @atomic(attempts=2)
    def work():
        calls["n"] += 1
        if calls["n"] < 2:
            raise _Serialization("could not serialize access")
        return calls["n"]

    assert work() == 2
    assert calls["n"] == 2
    assert _fake_db.rolled_back == 1
    assert _fake_db.committed == 1


def test_exhausting_attempts_reraises_last_error(_fake_db):
    """If every attempt deadlocks, the final error propagates and the
    closure ran exactly ``attempts`` times."""
    calls = {"n": 0}

    @atomic(attempts=3)
    def work():
        calls["n"] += 1
        raise _Deadlock(f"deadlock #{calls['n']}")

    with pytest.raises(_Deadlock):
        work()

    assert calls["n"] == 3
    assert _fake_db.rolled_back == 3
    assert _fake_db.committed == 0


# ── nested caller must NOT retry ────────────────────────────────────────


def test_nested_caller_does_not_retry(_fake_db):
    """A nested ``atomic`` is a savepoint inside an open outer transaction.
    It owns no outer transaction, so a deadlock inside it must re-raise
    immediately (run exactly once) — even with ``attempts > 1``."""
    inner_calls = {"n": 0}

    @atomic(attempts=5)
    def inner():
        inner_calls["n"] += 1
        raise _Deadlock("deadlock in nested")

    # Open the OUTER transaction first; ``inner`` then attaches as a savepoint.
    with pytest.raises(_Deadlock), atomic():  # outer owns the transaction
        inner()

    assert inner_calls["n"] == 1, (
        "a nested savepoint caller must not retry — it cannot roll the outer "
        "transaction back, so re-running its closure can't succeed"
    )


# ── attempts=1 preserves original behaviour ─────────────────────────────


def test_attempts_one_does_not_retry(_fake_db):
    """The default ``attempts=1`` is the original single-shot behaviour:
    a deadlock propagates after one run, no retry."""
    calls = {"n": 0}

    @atomic()  # attempts defaults to 1
    def work():
        calls["n"] += 1
        raise _Deadlock("deadlock")

    with pytest.raises(_Deadlock):
        work()

    assert calls["n"] == 1
    assert _fake_db.rolled_back == 1


def test_attempts_one_happy_path_commits(_fake_db):
    """attempts=1 still commits on success (no behaviour change)."""

    @atomic()
    def work():
        return 42

    assert work() == 42
    assert _fake_db.committed == 1
    assert _fake_db.rolled_back == 0


# ── non-retriable errors propagate without retry ────────────────────────


def test_non_retriable_error_is_not_retried(_fake_db):
    """A non-deadlock DB error (e.g. 23514 check violation) must NOT be
    retried even with attempts > 1 — only 40P01 / 40001 are transient."""
    calls = {"n": 0}

    @atomic(attempts=4)
    def work():
        calls["n"] += 1
        raise _NonRetriable("check constraint violated")

    with pytest.raises(_NonRetriable):
        work()

    assert calls["n"] == 1, "non-retriable error must propagate after one run"
    assert _fake_db.rolled_back == 1


def test_plain_exception_without_pgcode_not_retried(_fake_db):
    """An ordinary exception (no ``pgcode``) is never retriable."""
    calls = {"n": 0}

    @atomic(attempts=3)
    def work():
        calls["n"] += 1
        raise ValueError("not a DB error")

    with pytest.raises(ValueError):
        work()

    assert calls["n"] == 1


# ── attempts clamping + run() direct API ────────────────────────────────


def test_attempts_clamped_to_at_least_one():
    """``attempts<=0`` is clamped to 1 so a misconfigured value can't mean
    "never run the closure"."""
    assert Atomic(attempts=0).attempts == 1
    assert Atomic(attempts=-5).attempts == 1
    assert atomic(attempts=3).attempts == 3


def test_run_executes_and_returns(_fake_db):
    """``Atomic.run`` is the explicit retry-aware execution entry point."""
    result = atomic(attempts=2).run(lambda: "value")
    assert result == "value"
    assert _fake_db.committed == 1
