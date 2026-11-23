"""An UPDATE / DELETE never silently drops a clause that narrows it (ORM audit).

The write templates have no LIMIT, OFFSET or JOIN slot, and the grammar used
to discard the three without a word: ``where(...).limit(1).delete()`` deleted
EVERY matching row. The grammar now refuses the statement and names the
clause. ORDER BY does not change which rows are written and stays accepted.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar, SQLiteGrammar
from cara.exceptions import QueryException
from cara.testing.FacadeSwap import swap


@pytest.fixture(autouse=True)
def manager(tmp_path):
    path = tmp_path / "writes.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE users (id INTEGER PRIMARY KEY, team_id INTEGER, n INTEGER);
        CREATE TABLE teams (id INTEGER PRIMARY KEY);
        INSERT INTO users (id, team_id, n) VALUES (1, 1, 0), (2, 1, 0), (3, 2, 0);
        INSERT INTO teams (id) VALUES (1), (2);
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


def _narrowed(make, clause):
    builder = make().where("team_id", 1)
    if clause == "LIMIT":
        return builder.limit(1)
    if clause == "OFFSET":
        return builder.offset(1)
    return builder.join("teams", "users.team_id", "=", "teams.id")


@pytest.mark.parametrize("make", [_pg, _lite])
@pytest.mark.parametrize("clause", ["LIMIT", "OFFSET", "JOIN"])
def test_delete_refuses_a_clause_it_cannot_express(make, clause):
    with pytest.raises(QueryException, match=f"DELETE cannot express {clause}"):
        _narrowed(make, clause).delete(query=True).to_qmark()


@pytest.mark.parametrize("make", [_pg, _lite])
@pytest.mark.parametrize("clause", ["LIMIT", "OFFSET", "JOIN"])
def test_update_refuses_a_clause_it_cannot_express(make, clause):
    with pytest.raises(QueryException, match=f"UPDATE cannot express {clause}"):
        _narrowed(make, clause).update({"n": 1}, dry=True).to_qmark()


@pytest.mark.parametrize("make", [_pg, _lite])
def test_increment_refuses_a_limit(make):
    with pytest.raises(QueryException, match="UPDATE cannot express LIMIT"):
        make().where("team_id", 1).limit(1).increment("n", dry=True)


def test_every_dropped_clause_is_named():
    builder = (
        _pg()
        .where("team_id", 1)
        .join("teams", "users.team_id", "=", "teams.id")
        .limit(1)
        .offset(2)
    )
    with pytest.raises(QueryException, match="DELETE cannot express LIMIT, OFFSET, JOIN"):
        builder.delete(query=True).to_qmark()


def test_limit_zero_is_refused_not_read_as_no_limit():
    # ``0 == False``: an equality test against the "no limit" sentinel would
    # have let ``limit(0)`` (write nothing) through as an unbounded write.
    with pytest.raises(QueryException, match="LIMIT"):
        _pg().where("team_id", 1).limit(0).delete(query=True).to_qmark()


@pytest.mark.parametrize("make", [_pg, _lite])
def test_order_by_and_offset_zero_are_still_accepted(make):
    delete_sql = make().where("id", 1).order_by("id").offset(0).delete(query=True)
    assert delete_sql.to_qmark().startswith('DELETE FROM "users" WHERE')

    update_sql = make().where("id", 1).order_by("id").update({"n": 1}, dry=True)
    assert update_sql.to_qmark().startswith('UPDATE "users" SET "n" = \'?\'')


def test_a_limited_delete_deletes_nothing_instead_of_everything(manager):
    with pytest.raises(QueryException, match="LIMIT"):
        manager.table("users").where("team_id", 1).limit(1).delete()

    assert manager.table("users").count() == 3


def test_a_limited_update_writes_nothing_instead_of_everything(manager):
    with pytest.raises(QueryException, match="LIMIT"):
        manager.table("users").where("team_id", 1).limit(1).update({"n": 9})

    assert manager.table("users").where("n", 9).count() == 0
