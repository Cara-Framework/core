"""After a model UPDATE, the model holds what the row holds.

``_qb_update`` refreshed the model from ``materialized``: the values the
caller handed in, worked out BEFORE the statement compiled. Two values in
that same UPDATE are only decided later:

* ``TimeStampsScope`` adds ``updated_at`` while the query compiles, so it was
  never in ``materialized``. Every ``model.update()`` and ``model.save()`` left
  the model holding the OLD ``updated_at``. ``PATCH /api/products/{id}``
  returned a stamp the row no longer had.
* A column expression (``F("hits") + 1``) is worked out by the database from
  the current row. It was dropped from the refresh, so the model kept the
  value from before the write.

The UPDATE now asks for those columns back with ``RETURNING``. The model is
refreshed from the row the database actually wrote, and normalised the same
way ``hydrate`` normalises a read.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.configuration import Configuration
from cara.eloquent import DatabaseManager
from cara.eloquent.expressions import F, UpdateQueryExpression
from cara.eloquent.models.Model import Model
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar, SQLiteGrammar
from cara.testing.FacadeSwap import swap

_SEEDED_STAMP = "2026-01-01T00:00:00Z"


class _Note(Model):
    __table__ = "note"
    __fillable__ = ["title", "hits"]


class _Plain(Model):
    """A model that does not keep timestamps reads nothing back for them."""

    __table__ = "plain"
    __fillable__ = ["title"]
    __timestamps__ = False


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
        "hits INTEGER DEFAULT 0, created_at TEXT, updated_at TEXT)"
    )
    seed.execute(
        "INSERT INTO note VALUES (1, 'old', 0, ?, ?)", (_SEEDED_STAMP, _SEEDED_STAMP)
    )
    seed.execute("CREATE TABLE plain(id INTEGER PRIMARY KEY, title TEXT)")
    seed.execute("INSERT INTO plain VALUES (1, 'old')")
    seed.commit()
    seed.close()
    manager = DatabaseManager(
        "default", {"default": {"driver": "sqlite", "database": path}}
    )
    with swap("DB", manager):
        yield path


def _row(path: str, sql: str) -> tuple:
    with sqlite3.connect(path) as connection:
        return connection.execute(sql).fetchone()


def test_update_leaves_the_model_holding_the_updated_at_it_wrote(database):
    note = _Note.find(1)

    note.update({"title": "new"})

    stored = _Note.find(1).updated_at
    assert stored != note.get_new_date(_SEEDED_STAMP), "the scope stamped the row"
    # Pre-fix: the model still held 2026-01-01 while the row had moved on.
    assert note.updated_at == stored


def test_save_leaves_the_model_holding_the_updated_at_it_wrote(database):
    note = _Note.find(1)
    note.title = "saved"

    assert note.save() is True

    assert note.updated_at == _Note.find(1).updated_at
    assert note.get_dirty_attributes() == {}


def test_touch_leaves_the_model_holding_the_stamp_it_wrote(database):
    note = _Note.find(1)

    note.touch()

    stored = _Note.find(1).updated_at
    assert note.__attributes__["updated_at"] == stored
    assert note.__original_attributes__["updated_at"] == stored


def test_the_read_back_stamp_is_not_a_pending_change(database):
    note = _Note.find(1)

    note.update({"title": "new"})

    assert note.__original_attributes__["updated_at"] == note.updated_at


def test_a_column_expression_reads_back_what_the_database_computed(database):
    note = _Note.find(1)

    note.update({"hits": F("hits") + 2})

    assert _row(database, "SELECT hits FROM note WHERE id = 1") == (2,)
    # Pre-fix: the expression was stripped from the refresh and the model
    # kept the 0 it read before the write.
    assert note.hits == 2


def test_an_update_that_matched_no_row_keeps_the_stamp_the_row_had(database):
    """The stamp is read back, never assumed: an UPDATE that wrote nothing
    must not hand the model an ``updated_at`` no row ever held."""
    note = _Note.find(1)
    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM note WHERE id = 1")

    note.update({"title": "gone"})

    assert note.updated_at == note.get_new_date(_SEEDED_STAMP)


def test_a_model_without_timestamps_asks_for_nothing_back(database):
    plain = _Plain.find(1)

    plain.update({"title": "new"})

    assert plain.title == "new"
    assert "updated_at" not in plain.__attributes__


def test_a_dry_run_shows_the_read_back_the_update_sends(database):
    """A dry run renders the statement the write would send, RETURNING included."""
    note = _Note.find(1)

    sql = note.update({"title": "new"}, dry=True).to_sql()

    assert sql.endswith('RETURNING "updated_at"')


def test_a_model_without_timestamps_puts_no_returning_on_its_update(database):
    """Pinned on the SQL, not on the row. SQLite reads an unknown double-quoted
    name as a string literal, so ``RETURNING "updated_at"`` against a table
    with no such column passes here, while Postgres refuses the statement."""
    plain = _Plain.find(1)

    sql = plain.update({"title": "new"}, dry=True).to_sql()

    assert "RETURNING" not in sql


def test_an_unloaded_model_update_reads_nothing_back(database):
    """``Model.where(...).update()`` has no loaded instance to refresh, and a
    RETURNING there would ship every row the bulk UPDATE matched back."""
    sql = _Note.where("hits", 0).update({"title": "bulk"}, dry=True).to_sql()

    assert "RETURNING" not in sql


def test_a_table_update_still_answers_with_its_row_count(database):
    """Queue and outbox compare-and-set transitions read this count; a
    RETURNING clause here would turn it into a list of rows."""
    builder = QueryBuilder(grammar=SQLiteGrammar, connection="default", table="note")

    assert builder.where("id", 1).update({"title": "counted"}) == 1


@pytest.mark.parametrize("grammar", [PostgresGrammar, SQLiteGrammar])
def test_the_update_grammar_renders_the_columns_it_reads_back(grammar):
    compiled = grammar(table="note", updates=(UpdateQueryExpression({"title": "x"}),))
    compiled._returning = ("hits", "updated_at")

    sql = compiled.compile("update", qmark=True).to_sql()

    assert sql == 'UPDATE "note" SET "title" = \'?\' RETURNING "hits", "updated_at"'


@pytest.mark.parametrize("grammar", [PostgresGrammar, SQLiteGrammar])
def test_the_update_grammar_reads_nothing_back_unless_asked(grammar):
    compiled = grammar(table="note", updates=(UpdateQueryExpression({"title": "x"}),))

    sql = compiled.compile("update", qmark=True).to_sql()

    assert sql == 'UPDATE "note" SET "title" = \'?\''
