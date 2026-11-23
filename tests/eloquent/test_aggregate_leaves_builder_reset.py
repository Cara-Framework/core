"""An aggregate selects the aggregate alone, and leaves the builder reset.

``count()`` / ``sum()`` / ``exists()`` compile through ``to_qmark()``, which
resets the builder. Two defects sat around that call:

* Explicit select columns stayed beside the aggregate:
  ``select("id", "name").count()`` compiled ``SELECT id, name, COUNT(*)``.
  SQLite answers that; PostgreSQL refuses it ("column must appear in the
  GROUP BY clause"), so the same call worked in tests and failed in
  production.
* ``count()`` put the caller's ORDER BY back — and ``exists()`` the caller's
  columns and LIMIT — in a ``finally`` that ran AFTER the reset, so a reused
  builder carried them into its next query, without the wheres they belonged
  to (``test_query_builder_reset_completeness`` pins the reset contract).

The statements are captured on a stub connection; nothing is executed.
"""

from __future__ import annotations

import pytest

from cara.eloquent import DatabaseManager
from cara.exceptions import InvalidArgumentException


class _Recorder:
    """Stands in for a connection wrapper: records what it is asked to run."""

    transaction_level = 0

    def __init__(self):
        self.queries: list[str] = []

    def query(self, sql, bindings=(), results="*"):
        self.queries.append(sql)
        return {"m_count_result": 3, "m_sum_result": 7}


def _builder(driver: str):
    manager = DatabaseManager("app", {"app": {"driver": driver}})
    builder = manager.table("users")
    recorder = _Recorder()
    builder._connection = recorder
    return builder, recorder


@pytest.mark.parametrize("driver", ["sqlite", "postgres"])
def test_ungrouped_aggregate_drops_the_select_columns(driver):
    builder, recorder = _builder(driver)

    assert builder.select("id", "name").where("active", 1).count() == 3

    assert recorder.queries == [
        'SELECT COUNT(*) AS m_count_result FROM "users" WHERE "users"."active" = \'?\''
    ]


def test_grouped_aggregate_keeps_its_group_columns():
    builder, recorder = _builder("postgres")

    builder.select("team_id").group_by("team_id").sum("score")

    assert recorder.queries == [
        'SELECT "users"."team_id", SUM("users"."score") AS m_sum_result '
        'FROM "users" GROUP BY "users"."team_id"'
    ]


def test_distinct_aggregate_keeps_its_columns():
    builder, recorder = _builder("postgres")

    builder.select("team_id").distinct().count()

    assert recorder.queries[0].startswith('SELECT DISTINCT "users"."team_id", COUNT(*)')


def test_count_leaves_no_order_by_on_the_builder():
    builder, _ = _builder("postgres")

    builder.select("id").where("active", 1).order_by("name").count()

    assert builder._order_by == ()
    assert builder._columns == ()
    assert builder.to_sql() == 'SELECT * FROM "users"'


def test_exists_leaves_no_columns_or_limit_on_the_builder():
    builder, recorder = _builder("postgres")

    assert builder.select("id").where("active", 1).limit(5).exists() is True

    assert recorder.queries == [
        'SELECT 1 FROM "users" WHERE "users"."active" = \'?\' LIMIT 1'
    ]
    assert builder._columns == ()
    assert builder._limit is False
    assert builder.to_sql() == 'SELECT * FROM "users"'


def test_builder_is_handed_back_whole_when_compiling_fails():
    builder, recorder = _builder("postgres")
    builder.select("id").where("active", 1).order_by("name")
    order_by, columns, wheres = builder._order_by, builder._columns, builder._wheres

    def refuse():
        raise InvalidArgumentException("scope refused")

    builder.run_scopes = refuse
    with pytest.raises(InvalidArgumentException):
        builder.count()

    assert recorder.queries == []
    assert builder._order_by == order_by
    assert builder._columns == columns
    assert builder._wheres == wheres
