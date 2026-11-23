"""A builder identifier can never terminate its quoted identifier (ORM audit).

``where({key: v})``, ``group_by`` and the rest wrap a name in the dialect's
quote character through ``_table_column_string`` without escaping it, so
``where({'x"; DROP TABLE users; --': 1})`` closed the identifier and handed the
rest to the database as SQL. An embedded quote is now doubled (the SQL
standard escape inside a quoted identifier); every ordinary identifier still
compiles byte-identically, which ``test_grammar_compile_defects`` pins.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar, SQLiteGrammar
from cara.testing.FacadeSwap import swap

HOSTILE = 'x"; DROP TABLE users; --'


@pytest.fixture(autouse=True)
def manager(tmp_path):
    path = tmp_path / "identifiers.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'ann');
        """
    )
    raw.commit()
    raw.close()

    dm = DatabaseManager("app", {"app": {"driver": "sqlite", "database": str(path)}})
    with swap("DB", dm):
        yield dm


def _pg() -> QueryBuilder:
    return QueryBuilder(grammar=PostgresGrammar, connection="app", table="users")


def _lite() -> QueryBuilder:
    return QueryBuilder(grammar=SQLiteGrammar, connection="app", table="users")


@pytest.mark.parametrize("make", [_pg, _lite])
def test_where_dict_key_is_escaped(make):
    sql = make().where({HOSTILE: 1}).to_qmark()

    assert sql == (
        'SELECT * FROM "users" WHERE "users"."x""; DROP TABLE users; --" = \'?\''
    )


@pytest.mark.parametrize("make", [_pg, _lite])
def test_group_by_and_table_qualifier_are_escaped(make):
    assert make().group_by('a"b').to_qmark() == (
        'SELECT * FROM "users" GROUP BY "users"."a""b"'
    )
    assert make().where('t"x.c"y', 1).to_qmark() == (
        'SELECT * FROM "users" WHERE "t""x"."c""y" = \'?\''
    )


@pytest.mark.parametrize("make", [_pg, _lite])
def test_update_key_is_escaped(make):
    sql = make().where("id", 1).update({HOSTILE: 1}, dry=True).to_qmark()

    assert sql.startswith('UPDATE "users" SET "x""; DROP TABLE users; --" = \'?\'')


@pytest.mark.parametrize("make", [_pg, _lite])
def test_ordinary_identifiers_are_unchanged(make):
    assert make().select("id", "users.name").where("id", 1).to_qmark() == (
        'SELECT "users"."id", "users"."name" FROM "users" WHERE "users"."id" = \'?\''
    )
    assert make().select("*").to_qmark() == 'SELECT "users".* FROM "users"'


def test_the_hostile_name_reaches_the_database_as_one_identifier(manager):
    # The whole name is ONE quoted identifier: SQLite reports a missing
    # column, and the table is still there afterwards.
    with pytest.raises(Exception, match="no such column"):
        manager.table("users").where({HOSTILE: 1}).get()

    assert manager.table("users").count() == 1
