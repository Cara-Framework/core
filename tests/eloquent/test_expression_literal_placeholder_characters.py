r"""A literal operand of an ``F`` expression may hold ``?`` and ``%``.

Live defect: the literal is inlined into the statement TEXT, and two readers
interpret that text again — the connection turns every ``'?'`` into a driver
placeholder, and psycopg2 reads ``%`` as a format directive whenever the
statement carries bindings. So ``where(F("code"), "=", "?")`` consumed
another clause's binding, and ``"50%"`` beside any bound value raised inside
the driver. Each dialect now spells those two characters so neither reader
sees them: PostgreSQL as an escape string (``E'50\x25'``), SQLite as a
concatenation (``('a' || char(63) || 'b')``). Every other literal compiles
byte for byte as before.

SQL is compiled with both grammars; the SQLite form is also executed against
a file-backed SQLite database.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.expressions import F
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar, SQLiteGrammar
from cara.testing.FacadeSwap import swap


@pytest.fixture(autouse=True)
def manager(tmp_path):
    path = tmp_path / "literals.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE codes (id INTEGER PRIMARY KEY, code TEXT);
        INSERT INTO codes (id, code) VALUES
            (1, '?'), (2, 'a?b'), (3, '50%'), (4, 'plain'), (5, 'o''brien?');
        """
    )
    raw.commit()
    raw.close()

    dm = DatabaseManager("app", {"app": {"driver": "sqlite", "database": str(path)}})
    with swap("DB", dm):
        yield dm


def _pg() -> QueryBuilder:
    return QueryBuilder(grammar=PostgresGrammar, connection="app", table="codes")


def _lite() -> QueryBuilder:
    return QueryBuilder(grammar=SQLiteGrammar, connection="app", table="codes")


def _compiled(builder: QueryBuilder) -> tuple[str, list]:
    sql = builder.to_qmark()
    return sql, list(builder._bindings)


def _ids(rows) -> list[int]:
    return sorted(row["id"] for row in rows)


# ── PostgreSQL: an escape string carries neither character ──────────


@pytest.mark.parametrize(
    ("text", "literal"),
    [
        ("?", r"E'\x3F'"),
        ("50%", r"E'50\x25'"),
        ("it's 5%?", r"E'it''s 5\x25\x3F'"),
        ("a\\b%", r"E'a\\b\x25'"),  # the backslash is this form's escape
    ],
)
def test_postgres_spells_placeholder_characters_as_an_escape_string(text, literal):
    sql, bindings = _compiled(_pg().where("id", 7).where(F("code"), "=", text))

    assert sql == (
        f'SELECT * FROM "codes" WHERE "codes"."id" = \'?\' AND "codes"."code" = {literal}'
    )
    assert bindings == [7]
    assert "%" not in sql
    assert sql.count("?") == 1, "only the real binding's placeholder remains"


# ── SQLite: a concatenation without the character ───────────────────


def test_sqlite_spells_a_question_mark_as_char_63():
    sql, bindings = _compiled(_lite().where("id", 7).where(F("code"), "=", "a?b"))

    assert sql == (
        'SELECT * FROM "codes" WHERE "codes"."id" = \'?\' '
        "AND \"codes\".\"code\" = ('a' || char(63) || 'b')"
    )
    assert bindings == [7]


def test_sqlite_leaves_a_percent_sign_alone():
    """sqlite3 binds by ``?`` and never reads ``%``."""
    sql, _ = _compiled(_lite().where(F("code"), "=", "50%"))

    assert sql.endswith('"codes"."code" = \'50%\'')


@pytest.mark.parametrize(
    ("text", "expected"),
    [("?", [1]), ("a?b", [2]), ("50%", [3]), ("o'brien?", [5])],
)
def test_sqlite_executes_a_literal_beside_a_bound_value(manager, text, expected):
    rows = manager.table("codes").where("id", ">", 0).where(F("code"), "=", text).get()

    assert _ids(rows) == expected


# ── every other literal is byte-identical to before ─────────────────


@pytest.mark.parametrize("grammar", [PostgresGrammar, SQLiteGrammar])
@pytest.mark.parametrize(
    ("text", "literal"),
    [("plain", "'plain'"), ("o'brien", "'o''brien'"), ("", "''"), ("1", "'1'")],
)
def test_a_literal_without_placeholder_characters_keeps_its_plain_form(
    grammar, text, literal
):
    assert grammar(table="codes").expression_literal(text) == literal


def test_a_plain_literal_compiles_as_before_in_both_dialects():
    for make in (_pg, _lite):
        sql, bindings = _compiled(make().where(F("code"), "=", "o'brien"))

        assert sql == 'SELECT * FROM "codes" WHERE "codes"."code" = \'o\'\'brien\''
        assert bindings == []
