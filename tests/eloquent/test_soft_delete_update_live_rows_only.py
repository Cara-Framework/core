"""A query UPDATE on a soft-deleting model writes live rows only.

Live defect: SELECT and DELETE were scoped to live rows, UPDATE was not. A
model query could not SEE a soft-deleted row and could not delete it twice,
yet ``Model.where(...).update()`` / ``increment()`` / ``bulk_update()``
rewrote it — a "deactivate every listing of this product" also edited the
listings deleted last year, and a restore brought them back carrying a change
nobody made to a live row. ``SoftDeleteScope._update_live_rows`` now adds
``deleted_at IS NULL`` to a query UPDATE; ``with_trashed()`` reaches every
row, ``restore()`` and force-delete keep working, and a loaded instance's own
``save()`` stays keyed to its primary key.

Executed against a file-backed SQLite table; rows are read back raw, so no
scope can hide what was written.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.connections import SQLiteConnection
from cara.eloquent.models.Model import Model
from cara.eloquent.scopes import MakesSoftDeletes
from cara.testing.FacadeSwap import swap

TRASHED = {3, 6}
STAMP = "2026-01-01 00:00:00"


@pytest.fixture
def note(tmp_path):
    path = str(tmp_path / "soft_update.db")
    with sqlite3.connect(path) as seed:
        seed.execute(
            "CREATE TABLE note(id INTEGER PRIMARY KEY, label TEXT, hits INTEGER, "
            "deleted_at TEXT)"
        )
        seed.executemany(
            "INSERT INTO note(id, label, hits, deleted_at) VALUES (?, 'old', 0, ?)",
            [(row_id, STAMP if row_id in TRASHED else None) for row_id in range(1, 7)],
        )
    manager = DatabaseManager(
        "soft_update", {"soft_update": {"driver": "sqlite", "database": path}}
    )
    with swap("DB", manager):

        class Note(MakesSoftDeletes, Model):
            __table__ = "note"
            __connection__ = "soft_update"
            __fillable__ = ["label", "hits"]
            __timestamps__ = False

        Note.path = path
        yield Note


def _rows(note) -> dict[int, tuple]:
    """``id -> (label, hits, deleted_at)``, read past every scope."""
    with sqlite3.connect(note.path) as raw:
        return {
            row[0]: row[1:]
            for row in raw.execute("SELECT id, label, hits, deleted_at FROM note")
        }


def _labels(note) -> dict[int, str]:
    return {row_id: row[0] for row_id, row in _rows(note).items()}


def test_a_query_update_skips_trashed_rows(note):
    note.where("id", ">", 0).update({"label": "new"})

    labels = _labels(note)
    assert {labels[row_id] for row_id in TRASHED} == {"old"}, (
        "a query update rewrote rows in the trash"
    )
    assert {labels[row_id] for row_id in (1, 2, 4, 5)} == {"new"}


def test_a_query_increment_skips_trashed_rows(note):
    note.where("id", ">", 0).increment("hits", 5)

    hits = {row_id: row[1] for row_id, row in _rows(note).items()}
    assert hits == {1: 5, 2: 5, 3: 0, 4: 5, 5: 5, 6: 0}


@pytest.fixture
def sent(monkeypatch) -> list[str]:
    """Statements handed to the connection, not run: ``bulk_update`` writes
    PostgreSQL's ``UPDATE … FROM (VALUES …)``, which SQLite cannot parse."""
    statements: list[str] = []

    def record(self, query, bindings=(), results="*"):
        statements.append(query)
        return 0

    monkeypatch.setattr(SQLiteConnection, "query", record)
    return statements


def test_a_bulk_update_skips_trashed_rows(note, sent):
    note.where("id", ">", 0).bulk_update([{"id": 2, "label": "two"}])

    (statement,) = sent
    assert statement.startswith('UPDATE "note" SET "label" = _bulk."label" FROM')
    where = " ".join(statement.split(" WHERE ", 1)[1].split())
    assert where == (
        '"note"."id" = _bulk."id" AND ("note"."id" > \'?\' '
        'AND "note"."deleted_at" IS NULL)'
    ), "bulk_update reaches trashed rows"


def test_a_bulk_update_with_trashed_reaches_every_row(note, sent):
    note.with_trashed().where("id", ">", 0).bulk_update([{"id": 3, "label": "x"}])

    (statement,) = sent
    assert statement.endswith('WHERE "note"."id" = _bulk."id" AND ("note"."id" > \'?\')')


def test_with_trashed_reaches_every_row(note):
    note.with_trashed().where("id", ">", 0).update({"label": "all"})

    assert set(_labels(note).values()) == {"all"}


def test_only_trashed_writes_only_the_trash(note):
    note.only_trashed().where("id", ">", 0).update({"label": "binned"})

    labels = _labels(note)
    assert {row_id for row_id, label in labels.items() if label == "binned"} == TRASHED


def test_restore_still_brings_a_trashed_row_back(note):
    note.where("id", 3).restore()

    assert _rows(note)[3][2] is None
    assert _rows(note)[6][2] == STAMP


def test_force_delete_still_removes_a_trashed_row(note):
    note.where("id", 3).force_delete()

    assert 3 not in _rows(note)
    assert len(_rows(note)) == 5


def test_a_loaded_instance_save_is_keyed_to_its_primary_key(note):
    """An instance read ``with_trashed()`` can still be edited: its own
    ``save()`` gains no ``deleted_at IS NULL``."""
    trashed = note.with_trashed().find(3)
    trashed.label = "edited in the bin"
    trashed.save()

    assert _rows(note)[3] == ("edited in the bin", 0, STAMP)
    assert _labels(note)[6] == "old"
