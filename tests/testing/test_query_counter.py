"""``count_queries`` counts every statement that reaches the driver cursor."""

from __future__ import annotations

import pytest

from cara.eloquent.connections.BaseConnection import BaseConnection
from cara.testing import QueryCounter, assert_query_count, count_queries


class _Cursor:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, query, bindings) -> None:
        self.executed.append(query)


class _Connection(BaseConnection):
    full_details: dict = {}

    def __init__(self) -> None:
        self._cursor = _Cursor()


def test_counts_each_statement_and_restores_the_driver_method() -> None:
    original = BaseConnection.statement
    connection = _Connection()
    with count_queries() as counter:
        connection.statement("SELECT 1")
        connection.statement("SELECT\n   2")
    assert isinstance(counter, QueryCounter)
    assert counter.count == 2
    assert counter.queries == ["SELECT 1", "SELECT 2"]
    assert connection._cursor.executed == ["SELECT 1", "SELECT\n   2"]
    assert BaseConnection.statement is original


def test_nested_counters_each_see_only_their_own_scope() -> None:
    connection = _Connection()
    with count_queries() as outer:
        connection.statement("SELECT 1")
        with count_queries() as inner:
            connection.statement("SELECT 2")
        connection.statement("SELECT 3")
    assert (outer.count, inner.count) == (3, 1)


def test_ceiling_fails_with_the_captured_queries() -> None:
    connection = _Connection()
    with (
        pytest.raises(AssertionError, match="ceiling is 1") as raised,
        assert_query_count(maximum=1, label="loop"),
    ):
        for row in range(3):
            connection.statement(f"SELECT {row}")
    assert "SELECT 2" in str(raised.value)


def test_the_driver_method_is_restored_when_the_block_raises() -> None:
    original = BaseConnection.statement
    with pytest.raises(RuntimeError), count_queries():
        raise RuntimeError("boom")
    assert BaseConnection.statement is original
