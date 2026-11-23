"""Statements the grammar compiled wrong (ORM audit, 2026-11-23).

Each was either a statement the database refuses, or — worse — one it
accepts and answers differently from what the builder call says:

* a boolean value dropped its operator: ``where("active", "!=", True)``
  compiled to ``active IS True`` and returned the rows it should exclude;
* a second ``having()`` repeated the keyword (``HAVING a HAVING b``);
* a second ``on()`` of a join was glued on with the comparison operator
  (``ON a = b = c = d``);
* an empty nested group compiled to ``WHERE ()``;
* ``regexp`` / ``not regexp`` compiled to a ``REGEXP`` keyword PostgreSQL
  does not have;
* a literal operand of an ``F`` expression was spliced in unescaped, on the
  executed path too (``where(F("name"), "=", "x' OR '1'='1")`` matched every
  row);
* the executed statement went through the display cleanup, which rewrites
  ``,)`` to ``)`` inside the caller's raw fragments.

SQL is compiled with both grammars; the cases whose pre-fix form SQLite
accepted are also executed against a file-backed SQLite database.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.expressions import F, Greatest
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar, SQLiteGrammar
from cara.testing.FacadeSwap import swap


@pytest.fixture(autouse=True)
def manager(tmp_path):
    path = tmp_path / "grammar.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER,
                            team_id INTEGER);
        CREATE TABLE teams (id INTEGER PRIMARY KEY, owner_id INTEGER);
        INSERT INTO users (id, name, active, team_id) VALUES
            (1, 'ann', 1, 1), (2, 'bob', 0, 1), (3, 'o''brien', 1, 2), (4, 'dee', 0, 2);
        INSERT INTO teams (id, owner_id) VALUES (1, 1), (2, 9);
        """
    )
    raw.commit()
    raw.close()

    dm = DatabaseManager("app", {"app": {"driver": "sqlite", "database": str(path)}})
    with swap("DB", dm):
        yield dm


def _pg(table: str = "users") -> QueryBuilder:
    return QueryBuilder(grammar=PostgresGrammar, connection="app", table=table)


def _lite(table: str = "users") -> QueryBuilder:
    return QueryBuilder(grammar=SQLiteGrammar, connection="app", table=table)


def _compiled(builder: QueryBuilder) -> tuple[str, list]:
    sql = builder.to_qmark()
    return sql, list(builder._bindings)


def _ids(rows) -> list[int]:
    return sorted(row["id"] for row in rows)


# ── boolean values keep their operator ──────────────────────────────


@pytest.mark.parametrize("make", [_pg, _lite])
@pytest.mark.parametrize("operator", ["!=", "<>"])
@pytest.mark.parametrize("value", [True, False])
def test_boolean_value_keeps_its_operator(make, operator, value):
    sql, bindings = _compiled(make().where("active", operator, value))

    assert sql == f'SELECT * FROM "users" WHERE "users"."active" {operator} \'?\''
    assert bindings == [value]


def test_boolean_equality_keeps_its_shortcut():
    assert _compiled(_pg().where("active", True)) == (
        'SELECT * FROM "users" WHERE "users"."active" IS True',
        [],
    )
    assert _compiled(_lite().where("active", "=", False)) == (
        'SELECT * FROM "users" WHERE "users"."active" = \'0\'',
        [],
    )


def test_or_where_boolean_keeps_its_operator():
    sql, bindings = _compiled(_pg().where("id", 1).or_where("active", "!=", False))

    assert sql.endswith('OR "users"."active" != \'?\'')
    assert bindings == [1, False]


def test_where_not_null_still_binds_nothing():
    assert _compiled(_pg().where_not_null("name").where("id", 3)) == (
        'SELECT * FROM "users" WHERE "users"."name" IS NOT NULL AND "users"."id" = \'?\'',
        [3],
    )


def test_not_equal_true_excludes_the_true_rows(manager):
    assert _ids(manager.table("users").where("active", "!=", True).get()) == [2, 4]
    assert _ids(manager.table("users").where("active", "!=", False).get()) == [1, 3]
    assert _ids(manager.table("users").where("active", True).get()) == [1, 3]


# ── HAVING ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("make", [_pg, _lite])
def test_a_second_having_is_anded_onto_the_first(make):
    builder = (
        make()
        .select("team_id")
        .group_by("team_id")
        .having("team_id", ">", 0)
        .having("team_id", "<", 5)
        .having_raw("COUNT(*) > 1")
    )
    sql, bindings = _compiled(builder)

    assert sql.endswith(
        'GROUP BY "users"."team_id" HAVING "users"."team_id" > \'?\' '
        'AND "users"."team_id" < \'?\' AND COUNT(*) > 1'
    )
    assert bindings == [0, 5]


def test_two_havings_execute(manager):
    rows = (
        manager.table("users")
        .select("team_id")
        .group_by("team_id")
        .having("team_id", ">", 1)
        .having("team_id", "<", 5)
        .get()
    )

    assert [row["team_id"] for row in rows] == [2]


# ── JOIN ... ON a AND b ─────────────────────────────────────────────


def _two_conditions(join):
    return join.on("users.team_id", "=", "teams.id").on("users.id", "=", "teams.owner_id")


@pytest.mark.parametrize("make", [_pg, _lite])
def test_a_second_on_condition_is_anded(make):
    sql, _ = _compiled(make().join("teams", _two_conditions))

    assert sql == (
        'SELECT * FROM "users" INNER JOIN "teams" '
        'ON "users"."team_id" = "teams"."id" AND "users"."id" = "teams"."owner_id"'
    )


def test_two_on_conditions_execute(manager):
    rows = manager.table("users").select("users.id").join("teams", _two_conditions).get()

    # Only ann is both in team 1 and its owner.
    assert _ids(rows) == [1]


# ── empty groups ────────────────────────────────────────────────────


@pytest.mark.parametrize("make", [_pg, _lite])
def test_an_empty_group_is_no_predicate(make):
    assert _compiled(make().where(lambda q: q).where("id", 1)) == (
        'SELECT * FROM "users" WHERE "users"."id" = \'?\'',
        [1],
    )
    assert _compiled(make().where("id", 1).or_where(lambda q: q)) == (
        'SELECT * FROM "users" WHERE "users"."id" = \'?\'',
        [1],
    )
    assert _compiled(make().where(lambda q: q)) == ('SELECT * FROM "users"', [])


def test_a_group_with_predicates_is_still_parenthesised():
    sql, bindings = _compiled(
        _pg().where("id", 1).where(lambda q: q.where("a", 2).or_where("b", 3))
    )

    assert sql.endswith('AND ("users"."a" = \'?\' OR "users"."b" = \'?\')')
    assert bindings == [1, 2, 3]


# ── regexp ──────────────────────────────────────────────────────────


def test_postgres_regexp_uses_its_match_operators():
    assert _compiled(_pg().where("name", "regexp", "^a")) == (
        'SELECT * FROM "users" WHERE "users"."name" ~ \'?\'',
        ["^a"],
    )
    assert _compiled(_pg().where("name", "not regexp", "^a")) == (
        'SELECT * FROM "users" WHERE "users"."name" !~ \'?\'',
        ["^a"],
    )


def test_sqlite_regexp_still_runs_through_its_registered_function(manager):
    assert _ids(manager.table("users").where("name", "regexp", "^(a|b)").get()) == [1, 2]


# ── literal operands of F expressions ───────────────────────────────


@pytest.mark.parametrize("make", [_pg, _lite])
def test_expression_literal_is_escaped(make):
    sql, bindings = _compiled(make().where(F("name"), "=", "x' OR '1'='1"))

    assert sql == (
        "SELECT * FROM \"users\" WHERE \"users\".\"name\" = 'x'' OR ''1''=''1'"
    )
    assert bindings == []


def test_expression_literal_inside_an_update_is_escaped():
    builder = _pg().where("id", 1)
    builder.update({"name": Greatest(F("name"), "zz', active='9")}, dry=True)
    sql, bindings = _compiled(builder)

    assert sql == (
        "UPDATE \"users\" SET \"name\" = GREATEST(\"name\", 'zz'', active=''9') "
        "WHERE \"id\" = '?'"
    )
    assert bindings == [1]


def test_injected_expression_literal_matches_nothing(manager):
    table = manager.table("users")

    assert table.where(F("name"), "=", "x' OR '1'='1").get() == []
    assert _ids(manager.table("users").where(F("name"), "=", "o'brien").get()) == [3]


def test_numeric_expression_literal_renders_as_before():
    builder = _pg().where("id", 5)
    builder.update({"hits": F("hits") + 1}, dry=True)

    assert builder.to_qmark() == (
        'UPDATE "users" SET "hits" = "hits" + \'1\' WHERE "id" = \'?\''
    )


# ── the executed statement keeps raw fragments byte for byte ────────


@pytest.mark.parametrize("make", [_pg, _lite])
def test_executed_statement_keeps_raw_fragments(make):
    builder = make().select_raw("regexp_replace(name, '[(,)]', '')")
    sql, _ = _compiled(builder.where_raw("name = 'a,)b'"))

    assert "'[(,)]'" in sql
    assert "'a,)b'" in sql
