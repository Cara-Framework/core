"""Eager loading is one query per relation and linear in the rows it maps.

Runs the real builder → hydrate → eager → register pipeline against a
file-backed SQLite database (an ``:memory:`` database would be a fresh,
empty database on every connection) and counts the statements that reach it.

Pins:
* ``with_`` of to-one and to-many relations over N parents issues exactly
  1 + one query per relation, and every parent gets the right rows.
* Registering a ``has_one`` over N parents and M children is O(N + M) key
  resolutions, not the O(N x M) scan it used to be.
* A zero-row eager load keyed on a TEXT column registers an empty value
  instead of raising TypeError (a str looked up in a list-backed Collection).
* A single parent's zero-row ``has_many`` eager load is an empty Collection,
  the same as the lazy path, not ``None``.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.connections.SQLiteConnection import SQLiteConnection
from cara.eloquent.models.Model import Model
from cara.eloquent.relationships import belongs_to, has_many, has_one
from cara.support import Collection
from cara.testing.FacadeSwap import swap

PARENTS = 60


class _Owner(Model):
    __table__ = "owner"
    __fillable__ = ["*"]
    __timestamps__ = False


class _Profile(Model):
    __table__ = "profile"
    __fillable__ = ["*"]
    __timestamps__ = False


class _Child(Model):
    __table__ = "child"
    __fillable__ = ["*"]
    __timestamps__ = False


class _Tag(Model):
    __table__ = "tag"
    __fillable__ = ["*"]
    __timestamps__ = False


class _Parent(Model):
    __table__ = "parent"
    __fillable__ = ["*"]
    __timestamps__ = False

    @belongs_to("owner_id", "id")
    def owner(self):
        return _Owner

    @has_one("parent_id", "id")
    def profile(self):
        return _Profile

    @has_many("parent_id", "id")
    def children(self):
        return _Child

    @has_many("parent_code", "code")
    def tags(self):
        return _Tag


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "eager.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE owner (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT, owner_id INTEGER);
        CREATE TABLE profile (id INTEGER PRIMARY KEY, parent_id INTEGER, bio TEXT);
        CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, n INTEGER);
        CREATE TABLE tag (id INTEGER PRIMARY KEY, parent_code TEXT);
        """
    )
    raw.executemany(
        "INSERT INTO owner (id, name) VALUES (?, ?)",
        [(i, f"o{i}") for i in range(1, 6)],
    )
    raw.executemany(
        "INSERT INTO parent (id, code, owner_id) VALUES (?, ?, ?)",
        [(i, f"P-{i}", (i % 5) + 1 if i % 7 else None) for i in range(1, PARENTS + 1)],
    )
    raw.executemany(
        "INSERT INTO profile (parent_id, bio) VALUES (?, ?)",
        [(i, f"bio{i}") for i in range(1, PARENTS + 1) if i % 3],
    )
    raw.executemany(
        "INSERT INTO child (parent_id, n) VALUES (?, ?)",
        [(i, k) for i in range(1, PARENTS + 1) for k in range(i % 4)],
    )
    raw.commit()
    raw.close()

    dm = DatabaseManager("app", {"app": {"driver": "sqlite", "database": str(path)}})
    with swap("DB", dm):
        yield


@pytest.fixture
def statements(monkeypatch):
    seen: list[str] = []
    original = SQLiteConnection.statement

    def counting(self, query, bindings=()):
        seen.append(query)
        return original(self, query, bindings)

    monkeypatch.setattr(SQLiteConnection, "statement", counting)
    return seen


def test_with_loads_every_relation_in_one_query_each(database, statements):
    parents = _Parent.query().with_("owner", "profile", "children").get()

    assert len(statements) == 4, statements
    assert len(parents) == PARENTS

    for parent in parents:
        pid = parent.id
        owner = parent.owner
        if pid % 7:
            assert owner is not None and owner.id == (pid % 5) + 1
        else:
            assert owner is None

        profile = parent.profile
        if pid % 3:
            assert profile is not None and profile.bio == f"bio{pid}"
        else:
            assert profile is None

        assert sorted(c.n for c in parent.children) == list(range(pid % 4))

    # Every relation came from the eager cache: no lazy query ran.
    assert len(statements) == 4


def test_has_one_registration_is_linear_in_rows(database, monkeypatch):
    parents = _Parent.query().get()
    profiles = _Profile.query().get()
    relation = _Parent.__dict__["profile"]

    calls = {"n": 0}
    original = Collection._data_get

    def counting(self, item, key, default=None):
        calls["n"] += 1
        return original(self, item, key, default)

    monkeypatch.setattr(Collection, "_data_get", counting)

    mapped = relation.map_related(profiles)
    for parent in parents:
        relation.register_related("profile", parent, mapped)

    # One key resolution per child row; the per-parent scan was N x M.
    assert calls["n"] <= len(profiles) + len(parents), calls["n"]
    assert parents[0].profile.bio == "bio1"
    assert parents[2].profile is None


def test_empty_eager_load_on_text_key_registers_empty_collection(database, statements):
    parents = _Parent.query().with_("tags").get()

    assert len(statements) == 2
    assert all(isinstance(p.tags, Collection) and p.tags.count() == 0 for p in parents)


def test_single_parent_empty_has_many_is_empty_collection(database):
    parent = _Parent.query().where("id", 4).with_("children").first()

    assert isinstance(parent.children, Collection)
    assert parent.children.count() == 0


def test_group_by_is_insertion_ordered_and_none_safe():
    rows = [{"k": 2}, {"k": None}, {"k": 1}, {"k": 2}]

    grouped = Collection(rows).group_by("k").all()

    assert list(grouped) == [2, None, 1]
    assert grouped[2] == [{"k": 2}, {"k": 2}]
