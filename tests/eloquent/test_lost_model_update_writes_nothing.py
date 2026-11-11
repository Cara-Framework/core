"""A loaded model's UPDATE that matched no row reports it and changes nothing.

``_qb_update`` handed back the model whether or not the statement matched a
row. A model is always truthy, so ``save()`` answered ``True`` for a write
that never landed, though its docstring promised ``False``. And before it
returned, ``_qb_update`` filled the model with the caller's values, so a lost
compare-and-set (``row.where("status", seen).update(...)`` after another
writer moved the row) left the model claiming the values it had failed to
write, with nothing pending to show they were never stored.

Now the UPDATE's own answer decides: the rows ``RETURNING`` sends back, or
the matched-row count when nothing is read back. A miss returns ``None`` from
``update()`` and ``False`` from ``save()``, fires no ``updated``/``saved``
event, and leaves the model exactly as it was read; a ``save()``'s change
stays pending.

A query update with no loaded instance (``Model.where(...).update(...)``) had
the same lie in bulk: it answered with the throwaway model it built the query
on, truthy whether the UPDATE matched one row or none, so a bulk
compare-and-set always read as won. It now answers with the matched-row
count, as a ``DB.table(...)`` update always did.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.configuration import Configuration
from cara.decorators.Events import saved, updated
from cara.eloquent import DatabaseManager
from cara.eloquent.models.Model import Model
from cara.testing.FacadeSwap import swap

_SEEDED_STAMP = "2026-01-01T00:00:00Z"

#: Every model event and observer call the tests' models heard.
_HEARD: list[str] = []


class _Note(Model):
    """Timestamped: its UPDATE reads ``updated_at`` back with RETURNING."""

    __table__ = "note"
    __fillable__ = ["title"]

    @updated
    def _heard_updated(self):
        _HEARD.append("updated")

    @saved
    def _heard_saved(self):
        _HEARD.append("saved")


class _Plain(Model):
    """No timestamps and no expression: its UPDATE reads nothing back, so the
    connection answers with the matched-row count instead."""

    __table__ = "plain"
    __fillable__ = ["title"]
    __timestamps__ = False


class _Observer:
    def updated(self, model):
        _HEARD.append("observer.updated")


@pytest.fixture
def database(tmp_path):
    """A file database: every ``query()`` reopens the handle, and a
    ``:memory:`` database would vanish between statements."""
    if not Configuration._instance:
        Configuration.empty()
    path = str(tmp_path / "notes.sqlite")
    seed = sqlite3.connect(path)
    seed.execute(
        "CREATE TABLE note(id INTEGER PRIMARY KEY, title TEXT, "
        "created_at TEXT, updated_at TEXT)"
    )
    seed.execute("INSERT INTO note VALUES (1, 'old', ?, ?)", (_SEEDED_STAMP,) * 2)
    seed.execute("CREATE TABLE plain(id INTEGER PRIMARY KEY, title TEXT)")
    seed.execute("INSERT INTO plain VALUES (1, 'old')")
    seed.commit()
    seed.close()
    manager = DatabaseManager(
        "default", {"default": {"driver": "sqlite", "database": path}}
    )
    _HEARD.clear()
    _Note.observe(_Observer())
    try:
        with swap("DB", manager):
            yield path
    finally:
        Model.__observers__.pop(_Note, None)
        _HEARD.clear()


def _run(path: str, sql: str, *bindings) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(sql, bindings)


def _title(path: str, table: str) -> str | None:
    with sqlite3.connect(path) as connection:
        row = connection.execute(f"SELECT title FROM {table} WHERE id = 1").fetchone()
    return row[0] if row else None


@pytest.mark.parametrize("model", [_Note, _Plain], ids=["read-back", "row-count"])
def test_save_answers_false_when_the_update_matched_no_row(database, model):
    row = model.find(1)
    _run(database, f"DELETE FROM {model.__table__} WHERE id = 1")
    row.title = "saved"

    # Pre-fix: True — update() handed back the always-truthy model.
    assert row.save() is False


def test_a_lost_save_keeps_its_change_pending_and_fires_nothing(database):
    note = _Note.find(1)
    _run(database, "DELETE FROM note WHERE id = 1")
    note.title = "saved"

    note.save()

    assert note.get_dirty_attributes() == {"title": "saved"}
    assert note.__attributes__["title"] == "old"
    assert note.__original_attributes__["title"] == "old"
    assert note.__attributes__["updated_at"] == note.get_new_date(_SEEDED_STAMP)
    assert _HEARD == []


def test_a_saved_write_still_answers_true_and_fires_its_events(database):
    note = _Note.find(1)
    note.title = "saved"

    assert note.save() is True

    assert _title(database, "note") == "saved"
    assert note.get_dirty_attributes() == {}
    assert _HEARD == ["observer.updated", "updated", "saved"]


@pytest.mark.parametrize("model", [_Note, _Plain], ids=["read-back", "row-count"])
def test_a_lost_compare_and_set_leaves_the_model_as_it_was_read(database, model):
    """Another writer moved the row after this model read it: the CAS loses
    and the model must not claim the value it failed to write."""
    row = model.find(1)
    _run(database, f"UPDATE {model.__table__} SET title = 'theirs' WHERE id = 1")

    answer = row.where("title", "old").update({"title": "mine"})

    assert answer is None
    assert _title(database, model.__table__) == "theirs"
    # Pre-fix: "mine", in both the attributes and the originals.
    assert row.title == "old"
    assert row.__original_attributes__["title"] == "old"
    assert row.get_dirty_attributes() == {}
    assert "observer.updated" not in _HEARD


def test_a_won_compare_and_set_answers_with_the_written_model(database):
    note = _Note.find(1)

    answer = note.where("title", "old").update({"title": "mine"})

    assert answer is note
    assert note.title == "mine"
    assert note.updated_at == _Note.find(1).updated_at
    assert _HEARD == ["observer.updated"]


def test_touching_a_vanished_row_keeps_the_stamp_it_was_read_with(database):
    note = _Note.find(1)
    _run(database, "DELETE FROM note WHERE id = 1")

    note.touch()

    assert note.__attributes__["updated_at"] == note.get_new_date(_SEEDED_STAMP)


@pytest.mark.parametrize("model", [_Note, _Plain], ids=["timestamped", "plain"])
def test_a_model_query_update_answers_with_the_rows_it_matched(database, model):
    """The bulk compare-and-set: the first writer wins one row, the second,
    still expecting ``old``, matches none and must be told so."""
    won = model.where("id", 1).where("title", "old").update({"title": "mine"})
    lost = model.where("id", 1).where("title", "old").update({"title": "theirs"})

    # Pre-fix: both answered the throwaway model, truthy either way.
    assert (won, lost) == (1, 0)
    assert _title(database, model.__table__) == "mine"


def test_a_model_query_update_with_nothing_to_write_matched_nothing(database):
    """Every key the mass-assignment filter strips leaves no UPDATE to run; the
    answer is the zero rows it wrote, not a truthy model."""
    assert _Note.where("id", 1).update({"id": 7}) == 0
    assert _title(database, "note") == "old"
