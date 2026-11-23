"""``has`` / ``where_has`` / ``doesnt_have`` over a dotted relation path.

Each family carried its own copy of the dotted walk, and every copy started
from ``self._model.builder`` instead of ``self``. Three defects:

* on a ``clone()`` — and inside a ``where(lambda q: ...)`` group — the
  constraint landed on the model's ORIGINAL builder: the clone ran without
  it, the original gained it, and a grouped ``or_has`` escaped its group;
* ``where_doesnt_have("a.b", cb)`` negated the last level too (``NOT EXISTS
  (a WHERE NOT EXISTS (b AND cb))`` — "every a has such a b"), while
  ``doesnt_have("a.b")`` compiled the correct ``NOT EXISTS (a WHERE EXISTS
  (b))``;
* a three-level path repeated the OR / NOT variant on the middle level.

Compiled against a file-backed SQLite database; the ``where_doesnt_have``
case is also executed.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.models.Model import Model
from cara.eloquent.relationships import has_many
from cara.testing.FacadeSwap import swap


class _Toy(Model):
    __table__ = "toy"
    __fillable__ = ["*"]
    __timestamps__ = False


class _Kid(Model):
    __table__ = "kid"
    __fillable__ = ["*"]
    __timestamps__ = False

    @has_many("kid_id", "id")
    def toys(self):
        return _Toy


class _Parent(Model):
    __table__ = "parent"
    __fillable__ = ["*"]
    __timestamps__ = False

    @has_many("parent_id", "id")
    def kids(self):
        return _Kid


class _Street(Model):
    __table__ = "street"
    __fillable__ = ["*"]
    __timestamps__ = False

    @has_many("street_id", "id")
    def parents(self):
        return _Parent


_KIDS = 'SELECT * FROM "kid" WHERE "kid"."parent_id" = "parent"."id"'
_TOYS = 'SELECT * FROM "toy" WHERE "toy"."kid_id" = "kid"."id"'


@pytest.fixture(autouse=True)
def database(tmp_path):
    path = tmp_path / "paths.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE street (id INTEGER PRIMARY KEY);
        CREATE TABLE parent (id INTEGER PRIMARY KEY, street_id INTEGER);
        CREATE TABLE kid (id INTEGER PRIMARY KEY, parent_id INTEGER);
        CREATE TABLE toy (id INTEGER PRIMARY KEY, kid_id INTEGER, broken INTEGER);
        -- parent 1: a kid with a broken toy; parent 2: a kid whose toy is
        -- whole; parent 3: a kid without toys; parent 4: no kids.
        INSERT INTO parent (id, street_id) VALUES (1, 1), (2, 1), (3, 1), (4, 1);
        INSERT INTO kid (id, parent_id) VALUES (10, 1), (20, 2), (30, 3);
        INSERT INTO toy (id, kid_id, broken) VALUES (100, 10, 1), (200, 20, 0);
        """
    )
    raw.commit()
    raw.close()

    dm = DatabaseManager("app", {"app": {"driver": "sqlite", "database": str(path)}})
    with swap("DB", dm):
        yield


def test_a_dotted_has_on_a_clone_constrains_the_clone():
    base = _Parent.query().where("id", ">", 0)
    clone = base.clone().has("kids.toys")

    assert clone.to_sql() == (
        'SELECT * FROM "parent" WHERE "parent"."id" > \'0\' '
        f"AND EXISTS ({_KIDS} AND EXISTS ({_TOYS}))"
    )
    assert base.to_sql() == 'SELECT * FROM "parent" WHERE "parent"."id" > \'0\''


def test_a_dotted_or_has_inside_a_group_stays_in_the_group():
    sql = (
        _Parent.query()
        .where("street_id", 1)
        .where(lambda q: q.where("id", 4).or_has("kids.toys"))
        .to_sql()
    )

    assert sql == (
        'SELECT * FROM "parent" WHERE "parent"."street_id" = \'1\' '
        f'AND ("parent"."id" = \'4\' OR EXISTS ({_KIDS} AND EXISTS ({_TOYS})))'
    )


def test_where_doesnt_have_negates_the_first_level_only():
    def broken(query):
        return query.where("broken", 1)

    sql = _Parent.query().where_doesnt_have("kids.toys", broken).to_sql()

    assert sql == (
        f'SELECT * FROM "parent" WHERE NOT EXISTS ({_KIDS} '
        f'AND EXISTS ({_TOYS} AND "toy"."broken" = \'1\'))'
    )
    # "No kid of theirs has a broken toy": everyone but parent 1. The old
    # ``NOT EXISTS (kid WHERE NOT EXISTS (broken toy))`` answered [1, 4].
    rows = _Parent.query().where_doesnt_have("kids.toys", broken).get()
    assert sorted(row.id for row in rows) == [2, 3, 4]


def test_or_where_doesnt_have_joins_with_or_at_the_first_level_only():
    sql = (
        _Parent.query()
        .where("id", 1)
        .or_where_doesnt_have("kids.toys", lambda q: q.where("broken", 1))
        .to_sql()
    )

    assert sql == (
        'SELECT * FROM "parent" WHERE "parent"."id" = \'1\' '
        f'OR NOT EXISTS ({_KIDS} AND EXISTS ({_TOYS} AND "toy"."broken" = \'1\'))'
    )


@pytest.mark.parametrize(
    ("method", "opening"),
    [
        ("has", "WHERE EXISTS"),
        ("doesnt_have", "WHERE NOT EXISTS"),
    ],
)
def test_a_three_level_path_varies_the_first_level_only(method, opening):
    sql = getattr(_Street.query(), method)("parents.kids.toys").to_sql()

    assert sql == (
        f'SELECT * FROM "street" {opening} '
        '(SELECT * FROM "parent" WHERE "parent"."street_id" = "street"."id" '
        f"AND EXISTS ({_KIDS} AND EXISTS ({_TOYS})))"
    )


def test_a_three_level_or_has_does_not_or_the_inner_correlation():
    sql = _Street.query().where("id", 9).or_has("parents.kids.toys").to_sql()

    assert sql == (
        'SELECT * FROM "street" WHERE "street"."id" = \'9\' OR EXISTS '
        '(SELECT * FROM "parent" WHERE "parent"."street_id" = "street"."id" '
        f"AND EXISTS ({_KIDS} AND EXISTS ({_TOYS})))"
    )


def test_single_relation_forms_compile_as_before():
    assert _Parent.query().has("kids").to_sql() == (
        f'SELECT * FROM "parent" WHERE EXISTS ({_KIDS})'
    )
    assert _Parent.query().doesnt_have("kids").to_sql() == (
        f'SELECT * FROM "parent" WHERE NOT EXISTS ({_KIDS})'
    )
    assert _Parent.query().where_has("kids", lambda q: q.where("id", 10)).to_sql() == (
        f'SELECT * FROM "parent" WHERE EXISTS ({_KIDS} AND "kid"."id" = \'10\')'
    )
    with pytest.raises(AttributeError, match="not defined"):
        _Parent.query().has("ghosts")
