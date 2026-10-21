"""``cursor()`` streams exactly the rows ``get()`` would return.

Each chunk used to be a fresh ``QueryBuilder(model=...)`` with the wheres
copied over by hand. The constructor reseeds the model's global scopes, so a
scope the caller removed came back: ``only_trashed().cursor()`` compiled to
``deleted_at IS NOT NULL AND deleted_at IS NULL`` and yielded nothing (a
purge loop over it "succeeded" having purged no row), ``with_trashed()``
yielded only live rows, and the caller's own ``limit``/``offset`` were
overwritten by the chunk window.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.models.Model import Model
from cara.eloquent.scopes import MakesSoftDeletes
from cara.testing.FacadeSwap import swap

TRASHED = {3, 6, 9}


@pytest.fixture
def note(tmp_path):
    """A soft-deleting model over a file-backed SQLite table (every query
    opens its own connection, so ``:memory:`` would start empty each time)."""
    path = str(tmp_path / "cursor.db")
    with sqlite3.connect(path) as seed:
        seed.execute("CREATE TABLE note(id INTEGER PRIMARY KEY, deleted_at TEXT)")
        seed.executemany(
            "INSERT INTO note(id, deleted_at) VALUES (?, ?)",
            [
                (row_id, "2026-01-01 00:00:00" if row_id in TRASHED else None)
                for row_id in range(1, 11)
            ],
        )
    manager = DatabaseManager(
        "cursor_test", {"cursor_test": {"driver": "sqlite", "database": path}}
    )
    with swap("DB", manager):

        class Note(MakesSoftDeletes, Model):
            __table__ = "note"
            __connection__ = "cursor_test"

        yield Note


def _ids(rows) -> list[int]:
    return [int(row.id) for row in rows]


@pytest.mark.parametrize("chunk_size", [2, 4, 100])
def test_only_and_with_trashed_stream_what_get_returns(note, chunk_size):
    only = note.only_trashed().order_by("id")
    every = note.with_trashed().order_by("id")

    assert _ids(only.clone().cursor(chunk_size=chunk_size)) == _ids(only.get())
    assert _ids(every.clone().cursor(chunk_size=chunk_size)) == _ids(every.get())
    assert _ids(note.only_trashed().cursor(chunk_size=chunk_size)) == [3, 6, 9]


def test_the_default_scope_still_hides_trashed_rows(note):
    assert _ids(note.order_by("id").cursor(chunk_size=3)) == [1, 2, 4, 5, 7, 8, 10]


def test_without_global_scopes_drops_the_scope(note):
    rows = note.without_global_scopes().order_by("id").cursor(chunk_size=4)

    assert _ids(rows) == list(range(1, 11))


def test_the_callers_limit_and_offset_bound_the_stream(note):
    window = note.with_trashed().order_by("id").limit(5).offset(2)

    assert _ids(window.clone().cursor(chunk_size=2)) == [3, 4, 5, 6, 7]
    assert _ids(note.with_trashed().limit(0).cursor(chunk_size=2)) == []
