"""A ``has_many`` relation is a Collection on every load path.

The multi-parent eager path registered ``group_by``'s raw group — a plain
``list`` — for a parent WITH children, while a childless parent, a single
parent's eager load and the lazy path all yield a Collection. Code calling
``parent.children.count()`` worked in a test with no rows and raised
``AttributeError: 'list' object has no attribute 'count'`` in production.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.models.Model import Model
from cara.eloquent.relationships import has_many
from cara.support import Collection
from cara.testing.FacadeSwap import swap


class _Kid(Model):
    __table__ = "kid"
    __fillable__ = ["*"]
    __timestamps__ = False


class _Mom(Model):
    __table__ = "mom"
    __fillable__ = ["*"]
    __timestamps__ = False

    @has_many("mom_id", "id")
    def kids(self):
        return _Kid


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "shape.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE mom (id INTEGER PRIMARY KEY);
        CREATE TABLE kid (id INTEGER PRIMARY KEY, mom_id INTEGER, n INTEGER);
        INSERT INTO mom (id) VALUES (1), (2), (3);
        INSERT INTO kid (mom_id, n) VALUES (1, 10), (1, 11), (2, 20);
        """
    )
    raw.commit()
    raw.close()
    dm = DatabaseManager("app", {"app": {"driver": "sqlite", "database": str(path)}})
    with swap("DB", dm):
        yield


def test_multi_parent_eager_load_gives_each_parent_a_collection(database):
    moms = _Mom.query().with_("kids").order_by("id").get()

    shapes = {mom.id: type(mom.kids) for mom in moms}
    assert shapes == {1: Collection, 2: Collection, 3: Collection}
    assert [mom.kids.count() for mom in moms] == [2, 1, 0]
    assert sorted(moms[0].kids.pluck("n")) == [10, 11]


def test_every_path_agrees_on_the_type(database):
    eager_many = _Mom.query().with_("kids").order_by("id").get()[0].kids
    eager_one = _Mom.query().where("id", 1).with_("kids").first().kids
    lazy = _Mom.query().where("id", 1).first().kids

    assert type(eager_many) is type(eager_one) is type(lazy) is Collection
    assert eager_many.count() == eager_one.count() == lazy.count() == 2
