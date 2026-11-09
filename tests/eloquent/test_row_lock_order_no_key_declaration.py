"""``lock_for_update`` honours a model's ``__row_lock__`` declaration.

A parent row many tables reference (a workspace, a channel, a product) is locked
for mutual exclusion, not to stop children appearing. ``FOR UPDATE`` also blocks
the ``FOR KEY SHARE`` of every foreign key check on the row, so a transaction
holding one row and inserting a child waits on the parent while the holder of
the parent waits on that row, and Postgres aborts one of them.
``FOR NO KEY UPDATE`` still excludes every other locker.
"""

from __future__ import annotations

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.models import Model
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar, SQLiteGrammar
from cara.exceptions import InvalidArgumentException
from cara.testing.FacadeSwap import swap


@pytest.fixture(scope="module", autouse=True)
def _register_connections():
    dm = DatabaseManager(
        "row_lock",
        {"row_lock": {"driver": "sqlite", "database": ":memory:"}},
    )
    with swap("DB", dm):
        yield


class _Model:
    """Only what a builder reads off its model when compiling a SELECT."""

    _global_scopes: dict = {}
    __with__: tuple = ()
    __row_lock__ = "update"

    @staticmethod
    def get_selects():
        return ["id"]


class _Parent(_Model):
    __row_lock__ = "no_key_update"


def _builder(model, table="tenant", grammar=PostgresGrammar) -> QueryBuilder:
    return QueryBuilder(
        grammar=grammar, connection="row_lock", table=table, model=model()
    ).where("id", 1)


def test_a_model_that_declares_no_key_locks_its_rows_no_key():
    sql = _builder(_Parent).lock_for_update().to_sql()
    assert sql.rstrip().endswith("FOR NO KEY UPDATE"), sql


def test_the_base_model_keeps_a_full_row_lock():
    assert Model.__row_lock__ == "update"
    sql = _builder(_Model).lock_for_update().to_sql()
    assert sql.rstrip().endswith("FOR UPDATE"), sql


def test_a_builder_with_no_model_keeps_a_full_row_lock():
    sql = (
        QueryBuilder(grammar=PostgresGrammar, connection="row_lock", table="tenant")
        .where("id", 1)
        .lock_for_update()
        .to_sql()
    )
    assert sql.rstrip().endswith("FOR UPDATE"), sql
    assert "NO KEY" not in sql


@pytest.mark.parametrize(
    ("modifier", "tail"),
    [
        ({"skip_locked": True}, "FOR NO KEY UPDATE SKIP LOCKED"),
        ({"nowait": True}, "FOR NO KEY UPDATE NOWAIT"),
        ({"of": "tenant"}, 'FOR NO KEY UPDATE OF "tenant"'),
        ({"of": ["tenant"], "nowait": True}, 'FOR NO KEY UPDATE OF "tenant" NOWAIT'),
    ],
)
def test_every_modifier_still_compiles_on_a_no_key_lock(modifier, tail):
    sql = _builder(_Parent).lock_for_update(**modifier).to_sql()
    assert sql.rstrip().endswith(tail), sql


def test_skip_locked_and_nowait_stay_mutually_exclusive():
    with pytest.raises(InvalidArgumentException):
        _builder(_Parent).lock_for_update(skip_locked=True, nowait=True)


@pytest.mark.parametrize(
    ("of", "tail"),
    [
        ("job", 'FOR UPDATE OF "job"'),
        (["tenant", "job"], 'FOR UPDATE OF "tenant", "job"'),
    ],
)
def test_the_declaration_speaks_only_for_the_models_own_rows(of, tail):
    """``OF`` naming another table locks a row the declaration says nothing about."""
    sql = _builder(_Parent).lock_for_update(of=of).to_sql()
    assert sql.rstrip().endswith(tail), sql


def test_a_shared_lock_is_not_a_row_lock_for_update():
    sql = _builder(_Parent).shared_lock().to_sql()
    assert sql.rstrip().endswith("FOR SHARE"), sql


def test_sqlite_has_no_row_locks_to_compile():
    sql = _builder(_Parent, grammar=SQLiteGrammar).lock_for_update().to_sql()
    assert "FOR " not in sql, sql


def test_a_misspelled_declaration_fails_instead_of_taking_no_lock():
    """An unknown lock name compiles to nothing, so it must not reach the grammar."""

    class _Typo(_Model):
        __row_lock__ = "no_key"

    with pytest.raises(InvalidArgumentException):
        _builder(_Typo).lock_for_update()


def test_a_reused_builder_forgets_the_no_key_lock():
    builder = _builder(_Parent).lock_for_update()
    builder.reset()
    assert "FOR " not in builder.where("id", 2).to_sql()
