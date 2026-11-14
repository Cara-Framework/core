"""Cycle-2 Cara hunt: query-builder iteration and write guards (red-first).

Each test pins a promise a job relies on when it walks or mutates a table:

* ``chunk_by_id`` visits every matching row exactly once and stops. Today an
  ``or_where`` filter, a caller ``order_by`` or a qualified key column breaks
  that: the first two loop over the same rows forever (a backfill or sweep
  never finishes and repeats its side effects), the third stops after the
  first chunk and silently skips the rest.
* ``chunk`` pages with LIMIT/OFFSET, so without an ORDER BY PostgreSQL may
  return rows in a different order on each page: rows are skipped or seen
  twice. Laravel's ``chunk`` enforces an order by the primary key.
* ``having(column, ">", 0)`` keeps the operator and the zero. Today the zero
  is read as "no value" and the clause compiles to ``column = '>'``.
* ``increment`` / ``decrement`` refuse to run without a WHERE, exactly like
  ``update`` does; today ``increment("qty")`` on a bare builder rewrites
  every row of the table (stock counters included).

Runs the real builder against a file-backed SQLite database.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.connections.SQLiteConnection import SQLiteConnection
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar, SQLiteGrammar
from cara.exceptions import QueryException
from cara.testing.FacadeSwap import swap

ROWS = 10
# A callback that is called more often than this has looped: chunk size 3
# over 10 rows is 4 chunks at most.
LOOP_GUARD = 8


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "iteration.sqlite"
    raw = sqlite3.connect(path)
    raw.execute(
        "CREATE TABLE item (id INTEGER PRIMARY KEY, kind TEXT, name TEXT, qty INTEGER)"
    )
    raw.executemany(
        "INSERT INTO item (id, kind, name, qty) VALUES (?, ?, ?, ?)",
        [
            (i, "a" if i <= 4 else "b", f"n{ROWS + 1 - i:02d}", 5)
            for i in range(1, ROWS + 1)
        ],
    )
    raw.commit()
    raw.close()
    dm = DatabaseManager("s", {"s": {"driver": "sqlite", "database": str(path)}})
    with swap("DB", dm):
        yield path


def _items() -> QueryBuilder:
    return QueryBuilder(grammar=SQLiteGrammar, connection="s", table="item")


def _walk(builder: QueryBuilder, **kwargs) -> tuple[list[int], int]:
    seen: list[int] = []
    calls = {"n": 0}

    def callback(rows):
        calls["n"] += 1
        seen.extend(int(row["id"]) for row in rows)
        if calls["n"] >= LOOP_GUARD:
            return False
        return None

    builder.chunk_by_id(3, callback, **kwargs)
    return seen, calls["n"]


def test_chunk_by_id_with_or_where_visits_each_row_once(database):
    seen, calls = _walk(_items().where("kind", "a").or_where("kind", "b"))

    assert calls < LOOP_GUARD, f"chunk_by_id looped: {seen}"
    assert seen == list(range(1, ROWS + 1))


def test_chunk_by_id_ignores_a_caller_order_by(database):
    seen, calls = _walk(_items().order_by("name"))

    assert calls < LOOP_GUARD, f"chunk_by_id looped: {seen}"
    assert sorted(seen) == list(range(1, ROWS + 1))
    assert len(seen) == len(set(seen))


def test_chunk_by_id_with_qualified_column_reaches_the_last_row(database):
    seen, _ = _walk(_items(), column="item.id")

    assert seen == list(range(1, ROWS + 1)), "stopped after the first chunk"


def test_chunk_pages_in_a_stable_order(database, monkeypatch):
    queries: list[str] = []
    original = SQLiteConnection.query

    def recording(self, query, bindings=(), results="*"):
        queries.append(query)
        return original(self, query, bindings, results)

    monkeypatch.setattr(SQLiteConnection, "query", recording)

    _items().chunk(3, lambda rows: None)

    assert queries, "chunk issued no query"
    assert all("ORDER BY" in query.upper() for query in queries), queries


def test_having_keeps_operator_and_zero_value():
    dm = DatabaseManager("p", {"p": {"driver": "postgres", "database": "x"}})
    with swap("DB", dm):
        builder = (
            QueryBuilder(grammar=PostgresGrammar, connection="p", table="t")
            .group_by("sku")
            .having("units", ">", 0)
        )
        sql = builder.to_qmark()

    assert '"units" > ' in sql, sql
    assert list(builder._bindings) == [0]


@pytest.mark.parametrize("method", ["increment", "decrement"])
def test_counter_write_without_where_is_refused(database, method):
    with pytest.raises(QueryException):
        getattr(_items(), method)("qty", 1)

    raw = sqlite3.connect(database)
    try:
        quantities = {row[0] for row in raw.execute("SELECT qty FROM item")}
    finally:
        raw.close()
    assert quantities == {5}, "every row's counter was rewritten"
