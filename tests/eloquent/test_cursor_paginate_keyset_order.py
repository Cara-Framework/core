"""``cursor_paginate`` pages in the order its cursor seeks in.

Runs the real builder against a file-backed SQLite database and walks every
page.

Pre-fix the keyset ORDER BY was appended behind any order already on the
builder. With ``order_by("name")`` the pages came back sorted by ``name``
while the cursor sought past ``(id)``: a 12-row walk served 17 rows, repeated
most of them and never showed four. A table-qualified keyset column — the
form a joined query must use — read ``getattr(row, "event.rank")``, got
``None`` and raised "cursor column must be NOT NULL" on the first page.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.models.Model import Model
from cara.testing.FacadeSwap import swap

ROWS = 12
_FINGERPRINT = "0" * 64


class _Event(Model):
    __table__ = "event"
    __fillable__ = ["*"]
    __timestamps__ = False


@pytest.fixture(autouse=True)
def database(tmp_path, monkeypatch):
    # The cursor is signed with ``app.key``; no application is booted here.
    monkeypatch.setattr(
        "cara.http.Cursor.config", lambda key, default=None: "cursor-walk-secret-" * 3
    )
    path = tmp_path / "cursor.sqlite"
    raw = sqlite3.connect(path)
    raw.execute("CREATE TABLE event (id INTEGER PRIMARY KEY, name TEXT, rank INTEGER)")
    # ``name`` sorts opposite to ``id``; ``rank`` repeats (ties on the keyset).
    raw.executemany(
        "INSERT INTO event (id, name, rank) VALUES (?, ?, ?)",
        [(i, f"n{ROWS - i:02d}", i % 3) for i in range(1, ROWS + 1)],
    )
    raw.commit()
    raw.close()

    dm = DatabaseManager("app", {"app": {"driver": "sqlite", "database": str(path)}})
    with swap("DB", dm):
        yield


def _walk(make_query, **keyset) -> list[int]:
    seen: list[int] = []
    cursor = None
    for _ in range(ROWS + 1):  # a walk that never ends is a failure, not a hang
        page = make_query().cursor_paginate(
            5,
            cursor=cursor,
            scope="event.index",
            filter_fingerprint=_FINGERPRINT,
            **keyset,
        )
        seen.extend(row.id for row in page.result)
        cursor = page.next_cursor
        if cursor is None:
            return seen
    raise AssertionError(f"cursor walk did not terminate: {seen}")


def test_an_earlier_order_by_does_not_break_the_walk():
    seen = _walk(lambda: _Event.query().order_by("name"))

    assert seen == list(range(1, ROWS + 1))


def test_a_descending_walk_over_a_tied_column_shows_every_row_once():
    seen = _walk(lambda: _Event.query().order_by("name"), column="rank", direction="desc")

    assert sorted(seen) == list(range(1, ROWS + 1))
    assert len(seen) == ROWS
    ranks = [row_id % 3 for row_id in seen]
    assert ranks == sorted(ranks, reverse=True)


def test_a_table_qualified_keyset_column_is_read_from_the_row():
    seen = _walk(_Event.query, column="event.rank", primary_key="event.id")

    assert sorted(seen) == list(range(1, ROWS + 1))
    assert len(seen) == ROWS
