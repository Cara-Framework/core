"""What the ledger says a nullable column still owes, on real Postgres.

Needs a migrated scratch::

    DB_DATABASE=synk_scratch_<label> CARA_RUN_INTEGRATION=1 pytest <this file>
"""

from __future__ import annotations

import importlib
import os

import psycopg2
import psycopg2.extras
import pytest

_PLAN_MODULE = importlib.import_module("cara.commands.core.SchemaPlanCommand")

pytestmark = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and a migrated DB_DATABASE named synk_scratch_*",
)

_COLUMN = "sales_order.closed_at"
_FILL = f"{_COLUMN}:backfill"


class _RealDB:
    def __init__(self, connection):
        self.connection = connection

    def select(self, query, bindings=(), connection=None):
        with self.connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cursor:
            cursor.execute(query, bindings or None)
            return [dict(row) for row in cursor.fetchall()]

    def write(self, plan, key, kind, status, applied_at):
        with self.connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO schema_operation (plan_id, operation_key, kind, "
                "table_name, safety, forward_sql, restores_data, status, applied_at, "
                "created_at, updated_at) VALUES (%s, %s, %s, 'sales_order', 'additive', "
                "'-- x', false, %s, %s::timestamptz, NOW(), NOW())",
                [plan, key, kind, status, applied_at],
            )


@pytest.fixture
def db(monkeypatch):
    connection = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
    connection.autocommit = True
    real = _RealDB(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE TEMP TABLE schema_operation (LIKE public.schema_operation INCLUDING ALL)"
        )
    monkeypatch.setattr(_PLAN_MODULE, "DB", real)
    yield real
    connection.close()


def _owed():
    command = _PLAN_MODULE.SchemaPlanCommand.__new__(_PLAN_MODULE.SchemaPlanCommand)
    return command._read_owed_backfills()


def test_a_column_added_and_not_filled_is_owed(db):
    db.write("p1", _COLUMN, "add_column", "applied", "2026-11-09 10:00:00+00")

    assert _owed() == {_COLUMN}


def test_a_column_added_then_filled_is_not_owed(db):
    db.write("p1", _COLUMN, "add_column", "applied", "2026-11-09 10:00:00+00")
    db.write("p2", _FILL, "backfill_column", "applied", "2026-11-09 10:01:00+00")

    assert _owed() == set()


def test_a_fill_from_before_the_column_was_added_again_pays_nothing(db):
    """A deploy re-run puts the fill under a second plan id, so reversing the
    column's plan leaves that row ``applied``. The column came back empty and
    stopped before its fill, and the old row said it had been filled."""
    db.write("p2", _FILL, "backfill_column", "applied", "2026-11-09 10:01:00+00")
    db.write("p1", _COLUMN, "add_column", "applied", "2026-11-09 11:00:00+00")

    assert _owed() == {_COLUMN}
