"""``QueryBuilder.transaction()`` / ``begin()`` go through the resolver (ORM audit).

The builder used to ``begin()`` a private wrapper and commit or roll it back
directly. Nothing returned that wrapper to the pool afterwards (the permit
leak ``Atomic`` had, fixed in ``ConnectionResolver``), and the wrapper was
never pinned, so the writes inside ``with Model.query().transaction():`` ran
on sibling autocommit connections that the rollback could not undo.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.connections.ConnectionResolver import (
    ConnectionResolver,
    _get_registry,
)
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import SQLiteGrammar
from cara.exceptions import QueryException
from cara.testing.FacadeSwap import swap


@pytest.fixture
def manager(tmp_path):
    path = tmp_path / "builder_tx.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
    raw.commit()
    raw.close()

    _get_registry().clear()
    dm = DatabaseManager("app", {"app": {"driver": "sqlite", "database": str(path)}})
    with swap("DB", dm):
        yield dm
    _get_registry().clear()


@pytest.fixture
def closed(monkeypatch):
    seen: list[object] = []
    original = ConnectionResolver._safe_close

    def recording(connection):
        seen.append(connection)
        original(connection)

    monkeypatch.setattr(ConnectionResolver, "_safe_close", staticmethod(recording))
    return seen


def _builder() -> QueryBuilder:
    return QueryBuilder(grammar=SQLiteGrammar, connection="app", table="users")


def _names(manager) -> list[str]:
    return [row["name"] for row in manager.table("users").get()]


def test_context_manager_commits_and_returns_the_connection(manager, closed):
    builder = _builder()
    with builder.transaction():
        connection = _get_registry()["app"]
        manager.table("users").create({"name": "kept"})

    assert _names(manager) == ["kept"]
    assert closed == [connection]
    assert "app" not in _get_registry()
    assert builder._connection is None


def test_a_failure_rolls_back_the_writes_made_inside_the_block(manager, closed):
    with pytest.raises(RuntimeError), _builder().transaction():
        manager.table("users").create({"name": "undone"})
        raise RuntimeError("boom")

    # The insert ran on the builder's pinned connection, so it rolled back.
    assert _names(manager) == []
    assert len(closed) == 1
    assert "app" not in _get_registry()


def test_callback_form_commits_and_releases(manager, closed):
    result = _builder().transaction(
        lambda: manager.table("users").create({"name": "cb"}) and "done"
    )

    assert result == "done"
    assert _names(manager) == ["cb"]
    assert len(closed) == 1
    assert "app" not in _get_registry()


def test_begin_commit_by_hand_releases_the_connection(manager, closed):
    builder = _builder()
    builder.begin()
    manager.table("users").create({"name": "manual"})
    builder.commit()

    assert _names(manager) == ["manual"]
    assert len(closed) == 1
    with pytest.raises(QueryException, match="No active transaction to commit"):
        builder.commit()


def test_a_builder_transaction_nests_inside_an_open_one(manager, closed):
    with manager.transaction():
        with _builder().transaction():
            manager.table("users").create({"name": "inner"})
        # The inner level released only its savepoint.
        assert closed == []
        assert "app" in _get_registry()

    assert _names(manager) == ["inner"]
    assert len(closed) == 1
