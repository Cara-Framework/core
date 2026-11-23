"""``atomic()`` unwinds through the resolver at every level, as ``DB.transaction()`` does.

Three ways a transaction opened by ``atomic()`` used to stay pinned in the
context's registry — holding its pool permit, with every later statement of
that context running inside a transaction nobody would commit:

* the outermost COMMIT itself failed (a deferred constraint, a serialization
  failure reported at commit): ``DB.transaction()`` rolled back and unpinned,
  ``atomic()`` did nothing;
* a nested ``atomic`` called the connection directly, so a ``RELEASE
  SAVEPOINT`` that failed (PostgreSQL's 25P02 after a swallowed statement
  error) left the level un-decremented and the outer rollback stopped at the
  savepoint;
* the same bypass kept a rolled-back nested block's after-commit callbacks
  alive for the outer commit and never ran its after-rollback callbacks.

And one way a real error was replaced by a misleading one: an after-commit
callback that raised was answered with a rollback of a transaction that no
longer existed ("No active transaction found for connection").

The transaction-level cases run on a fake connection mirroring
``PostgresConnection``'s level math; the deferred-constraint case runs
against a real file-backed SQLite database.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.Atomic import atomic
from cara.eloquent.connections.ConnectionResolver import (
    _get_after_commit_registry,
    _get_after_rollback_registry,
    _get_registry,
)
from cara.testing.FacadeSwap import swap


class _CommitRefused(Exception):
    """The driver refusing a COMMIT (e.g. a deferred constraint)."""


class _ReleaseRefused(Exception):
    """PostgreSQL's 25P02: RELEASE SAVEPOINT inside an aborted transaction."""


class _CallbackFailed(Exception):
    pass


class _FakeConnection:
    """Mirrors ``PostgresConnection``'s transaction-level math."""

    def __init__(self, *, refuse_commit=False, refuse_release=False):
        self.transaction_level = 0
        self.open = 1
        self.rolled_back = 0
        self.closed = 0
        self._refuse_commit = refuse_commit
        self._refuse_release = refuse_release

    def begin(self):
        self.transaction_level += 1
        return self

    def commit(self):
        if self.transaction_level > 1:
            if self._refuse_release:
                # ``release_savepoint`` raises before it decrements.
                raise _ReleaseRefused("current transaction is aborted")
            self.transaction_level -= 1
            return self
        # The outermost level drops in a ``finally``, commit or not.
        self.transaction_level -= 1
        if self._refuse_commit:
            raise _CommitRefused("deferred constraint violated")
        return self

    def rollback(self):
        if self.transaction_level <= 0:
            return self
        self.rolled_back += 1
        self.transaction_level -= 1
        return self

    def close_connection(self):
        self.closed += 1


@pytest.fixture(autouse=True)
def _clean_registries():
    for registry in (
        _get_registry(),
        _get_after_commit_registry(),
        _get_after_rollback_registry(),
    ):
        registry.clear()
    yield
    for registry in (
        _get_registry(),
        _get_after_commit_registry(),
        _get_after_rollback_registry(),
    ):
        registry.clear()


def _bind(monkeypatch, conn) -> DatabaseManager:
    manager = DatabaseManager("app", {"app": {"driver": "sqlite"}})
    monkeypatch.setattr(
        manager._ensure_resolver(),
        "_create_connection_instance",
        lambda name: conn,
        raising=False,
    )
    return manager


def test_a_failed_outer_commit_is_rolled_back_and_unpinned(monkeypatch):
    conn = _FakeConnection(refuse_commit=True)
    manager = _bind(monkeypatch, conn)
    fired: list[str] = []

    with swap("DB", manager), pytest.raises(_CommitRefused), atomic():
        manager.after_commit(lambda: fired.append("committed"))
        manager.after_rollback(lambda: fired.append("rolled back"))

    assert "app" not in _get_registry()
    assert conn.closed == 1
    assert fired == ["rolled back"]
    assert _get_after_commit_registry().get("app") in (None, [])


def test_a_failed_savepoint_release_does_not_strand_the_outer_transaction(
    monkeypatch,
):
    conn = _FakeConnection(refuse_release=True)
    manager = _bind(monkeypatch, conn)

    with swap("DB", manager), pytest.raises(_ReleaseRefused), atomic(), atomic():
        pass  # a statement failed in here and was swallowed

    assert conn.transaction_level == 0
    assert "app" not in _get_registry()
    assert conn.closed == 1


def test_a_rolled_back_nested_block_takes_its_callbacks_with_it(monkeypatch):
    conn = _FakeConnection()
    manager = _bind(monkeypatch, conn)
    fired: list[str] = []

    with swap("DB", manager), atomic():
        manager.after_commit(lambda: fired.append("outer committed"))
        with pytest.raises(RuntimeError), atomic():
            manager.after_commit(lambda: fired.append("inner committed"))
            manager.after_rollback(lambda: fired.append("inner rolled back"))
            raise RuntimeError("inner failed")
        assert fired == ["inner rolled back"]

    assert fired == ["inner rolled back", "outer committed"]
    assert conn.closed == 1


def test_a_committed_nested_block_hands_its_callbacks_to_the_outer_commit(
    monkeypatch,
):
    conn = _FakeConnection()
    manager = _bind(monkeypatch, conn)
    fired: list[str] = []

    with swap("DB", manager), atomic():
        with atomic():
            manager.after_commit(lambda: fired.append("inner committed"))
        assert fired == []
        assert conn.transaction_level == 1

    assert fired == ["inner committed"]
    assert "app" not in _get_registry()


@pytest.mark.parametrize("door", ["atomic", "transaction"])
def test_a_failing_after_commit_callback_raises_its_own_error(monkeypatch, door):
    conn = _FakeConnection()
    manager = _bind(monkeypatch, conn)

    def fail():
        raise _CallbackFailed("mail queue down")

    with swap("DB", manager):
        block = atomic() if door == "atomic" else manager.transaction()
        with pytest.raises(_CallbackFailed), block:
            manager.after_commit(fail)

    # The commit landed and the connection went back before the callback ran.
    assert conn.rolled_back == 0
    assert conn.closed == 1
    assert "app" not in _get_registry()


def test_a_deferred_constraint_at_commit_leaves_the_context_usable(tmp_path):
    path = tmp_path / "deferred.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE parent (id INTEGER PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED
        );
        """
    )
    raw.commit()
    raw.close()
    manager = DatabaseManager(
        "app",
        {"app": {"driver": "sqlite", "database": str(path), "foreign_keys": True}},
    )

    with swap("DB", manager):
        # The orphan child is only refused when the transaction commits.
        with pytest.raises(sqlite3.IntegrityError), atomic():
            manager.statement("INSERT INTO child (id, parent_id) VALUES (1, 99)")

        assert "app" not in _get_registry()

        # Pre-fix this write joined the transaction the failed commit left
        # open, and was never visible to anyone.
        manager.statement("INSERT INTO parent (id) VALUES (7)")

    check = sqlite3.connect(path)
    try:
        assert check.execute("SELECT id FROM parent").fetchall() == [(7,)]
        assert check.execute("SELECT id FROM child").fetchall() == []
    finally:
        check.close()
