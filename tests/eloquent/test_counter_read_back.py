"""``increment`` / ``decrement`` write once and hand a loaded model its new value.

Runs the real builder against a file-backed SQLite database and counts the
statements that reach it.

Pre-fix, on every dialect:

* ``loaded.increment("n")`` ran its UPDATE and then raised ``TypeError:
  attribute name must be string, not 'int'`` — the post processor re-read the
  row with a second SELECT and asked ``column in result`` of the hydrated
  model, which Python answers by indexing the model with integers. The
  counter had already moved; the caller saw a failure.
* that re-read hard-coded ``id``, so a model keyed on another column failed
  with "no such column" after its write;
* ``Model.where(...).increment("n")`` raised ``AttributeError`` before
  writing anything: the builder asked the query's throwaway model instance
  for a primary key value.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.connections.SQLiteConnection import SQLiteConnection
from cara.eloquent.models.Model import Model
from cara.exceptions import QueryException
from cara.testing.FacadeSwap import swap


class _Counter(Model):
    __table__ = "counter"
    __fillable__ = ["*"]
    __timestamps__ = False


class _Keyed(Model):
    __table__ = "keyed"
    __primary_key__ = "code"
    __fillable__ = ["*"]
    __timestamps__ = False


@pytest.fixture
def manager(tmp_path):
    path = tmp_path / "counter.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER);
        INSERT INTO counter (id, n) VALUES (1, 5), (2, 5);
        CREATE TABLE keyed (code TEXT PRIMARY KEY, n INTEGER);
        INSERT INTO keyed (code, n) VALUES ('a', 1), ('b', 1);
        """
    )
    raw.commit()
    raw.close()

    dm = DatabaseManager("app", {"app": {"driver": "sqlite", "database": str(path)}})
    with swap("DB", dm):
        yield dm


@pytest.fixture
def statements(monkeypatch):
    seen: list[str] = []
    original = SQLiteConnection.statement

    def counting(self, query, bindings=()):
        seen.append(query)
        return original(self, query, bindings)

    monkeypatch.setattr(SQLiteConnection, "statement", counting)
    return seen


def _stored(manager, table: str, key: str, value) -> int:
    return manager.table(table).where(key, value).first()["n"]


def test_loaded_model_increment_returns_the_new_value_in_one_statement(
    manager, statements
):
    counter = _Counter.find(1)
    del statements[:]

    assert counter.increment("n") == 6
    assert counter.increment("n", 4) == 10
    assert counter.decrement("n", 3) == 7

    assert len(statements) == 3, statements
    assert all(statement.startswith("UPDATE") for statement in statements)
    assert _stored(manager, "counter", "id", 1) == 7
    assert _stored(manager, "counter", "id", 2) == 5


def test_loaded_model_keyed_on_another_column_reads_its_value_back(manager):
    keyed = _Keyed.query().where("code", "a").first()

    assert keyed.increment("n", 2) == 3
    assert _stored(manager, "keyed", "code", "a") == 3
    assert _stored(manager, "keyed", "code", "b") == 1


def test_model_query_increment_writes_the_matched_rows(manager):
    assert _Counter.where("id", 2).increment("n", 2) is None

    assert _stored(manager, "counter", "id", 2) == 7
    assert _stored(manager, "counter", "id", 1) == 5


def test_table_builder_increment_keeps_answering_none(manager):
    assert manager.table("counter").where("id", 1).decrement("n") is None

    assert _stored(manager, "counter", "id", 1) == 4


def test_unbounded_counter_is_still_refused(manager):
    with pytest.raises(QueryException, match="increment"):
        manager.table("counter").increment("n")
    with pytest.raises(QueryException, match="decrement"):
        _Counter.query().decrement("n")

    assert _stored(manager, "counter", "id", 1) == 5
