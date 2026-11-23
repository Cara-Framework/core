"""Statements the ORM used to send that it had no reason to send.

Runs the real builder -> hydrate -> eager pipeline against a file-backed
SQLite database and counts the statements that reach it (the harness of
``test_cara_core_eager_loading``).

Pins:
* ``cursor()`` eager-loads a relation once per CHUNK, not once per row;
* an eager load whose parents carry no key (every foreign key NULL) sends no
  query, for a collection and for a single parent alike;
* a NULL local key relates to nothing: a ``has_many`` / ``has_one`` parent
  without its key gets no rows instead of the table's orphans;
* the builder's ``first_or_create`` returns a found record without
  serializing it (casts, relations and ``__appends__`` accessors).
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

PARENTS = 25


class _Owner(Model):
    __table__ = "owner"
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

    @has_many("parent_id", "id")
    def children(self):
        return _Child

    @has_many("parent_code", "code")
    def tags(self):
        return _Tag

    @has_one("parent_code", "code")
    def first_tag(self):
        return _Tag


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "redundant.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE owner (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT, owner_id INTEGER);
        CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, n INTEGER);
        CREATE TABLE tag (id INTEGER PRIMARY KEY, parent_code TEXT);
        """
    )
    raw.executemany(
        "INSERT INTO owner (id, name) VALUES (?, ?)",
        [(i, f"o{i}") for i in range(1, 4)],
    )
    # Parents 21..25 have neither an owner nor a code.
    raw.executemany(
        "INSERT INTO parent (id, code, owner_id) VALUES (?, ?, ?)",
        [
            (i, f"P-{i}" if i <= 20 else None, (i % 3) + 1 if i <= 20 else None)
            for i in range(1, PARENTS + 1)
        ],
    )
    raw.executemany(
        "INSERT INTO child (parent_id, n) VALUES (?, ?)",
        [(i, k) for i in range(1, PARENTS + 1) for k in range(2)],
    )
    # Two tags of parent 1, and three orphans (no parent_code).
    raw.executemany(
        "INSERT INTO tag (parent_code) VALUES (?)",
        [("P-1",), ("P-1",), (None,), (None,), (None,)],
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


def test_cursor_eager_loads_once_per_chunk(database, statements):
    rows = list(_Parent.query().with_("owner", "children").cursor(chunk_size=10))

    # 3 pages (10 + 10 + 5), each followed by one query per relation — except
    # the last page's ``owner``, whose five parents carry no key. Per-row
    # eager loading sent 3 + 25 x 2 = 53.
    assert len(statements) == 3 + 3 * 2 - 1, statements
    assert [row.id for row in rows] == list(range(1, PARENTS + 1))
    for row in rows:
        assert [child.parent_id for child in row.children] == [row.id, row.id]
        if row.id <= 20:
            assert row.owner.id == (row.id % 3) + 1
        else:
            assert row.owner is None


def test_cursor_rows_stay_untagged_for_the_lazy_load_guard(database):
    row = next(iter(_Parent.query().cursor(chunk_size=5)))

    assert "_from_collection" not in row.__dict__


def test_eager_load_without_any_key_sends_no_query(database, statements):
    parents = _Parent.query().where("id", ">", 20).with_("owner", "tags").get()

    assert len(statements) == 1, statements
    assert len(parents) == 5
    for parent in parents:
        assert parent.owner is None
        assert isinstance(parent.tags, Collection)
        assert parent.tags.count() == 0


def test_single_parent_eager_load_with_a_null_key_sends_no_query(database, statements):
    parent = _Parent.query().where("id", 21).with_("owner", "tags", "first_tag").first()

    assert len(statements) == 1, statements
    assert parent.owner is None
    assert parent.first_tag is None
    assert isinstance(parent.tags, Collection)
    # ``WHERE parent_code IS NULL`` handed this parent the three orphan tags.
    assert parent.tags.count() == 0


def test_lazy_load_with_a_null_key_relates_to_nothing(database, statements):
    parent = _Parent.query().where("id", 21).first()
    del statements[:]

    assert parent.tags.count() == 0
    assert parent.first_tag is None
    assert statements == []

    keyed = _Parent.query().where("id", 1).first()
    assert keyed.tags.count() == 2
    assert keyed.first_tag.parent_code == "P-1"


def test_builder_first_or_create_returns_a_found_record_unserialized(
    database, monkeypatch
):
    def refuse(self, *args, **kwargs):
        raise AssertionError("a found record must not be serialized")

    monkeypatch.setattr(_Owner, "serialize", refuse)

    found = _Owner.query().first_or_create({"id": 2}, {"name": "ignored"})

    assert found.id == 2
    assert found.name == "o2"


def test_builder_first_or_create_still_creates_a_missing_record(database):
    created = _Owner.query().first_or_create({"id": 9}, {"name": "made"})

    assert created.name == "made"
    assert _Owner.query().where("id", 9).first().name == "made"


def test_load_sends_one_query_per_relation_and_none_for_the_model(database, statements):
    parent = _Parent.query().where("id", 1).first()
    del statements[:]

    assert parent.load("owner", "children") is parent

    # One per relation. ``load()`` used to re-SELECT the parent's own row
    # first, only to copy the relations off that copy: 3 statements.
    assert len(statements) == 2, statements
    assert all('FROM "parent"' not in sql for sql in statements), statements
    assert parent.owner.id == 2
    assert [child.parent_id for child in parent.children] == [1, 1]
    assert len(statements) == 2, statements


def test_load_uses_the_keys_the_instance_holds(database, statements):
    # Eager loading reads the in-memory keys (Laravel's ``load``), so an
    # instance the default scopes could no longer re-read still loads.
    parent = _Parent.hydrate({"id": 2, "code": "P-2", "owner_id": 3})

    parent.load("owner", "tags")

    assert parent.owner.id == 3
    assert parent.tags.count() == 0
    assert len(statements) == 2, statements
