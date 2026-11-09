"""``schema:apply`` and ``schema:rollback`` on real Postgres: locks and the ledger.

The ledger here is a session TEMP table shadowing ``schema_operation``, so
nothing is left in the database. Needs a migrated scratch::

    DB_DATABASE=synk_scratch_<label> CARA_RUN_INTEGRATION=1 pytest <this file>
"""

from __future__ import annotations

import importlib
import os
import time
import uuid
from contextlib import contextmanager

import psycopg2
import psycopg2.errors
import psycopg2.extras
import pytest

from cara.schema import ADDITIVE, LOCKING, Operation, plan_id

_APPLY_MODULE = importlib.import_module("cara.commands.core.SchemaApplyCommand")
_ROLLBACK_MODULE = importlib.import_module("cara.commands.core.SchemaRollbackCommand")

pytestmark = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and a migrated DB_DATABASE named synk_scratch_*",
)


class _RealDB:
    """The ``DB`` facade's three calls on one autocommit connection."""

    def __init__(self, connection):
        self.connection = connection

    @contextmanager
    def transaction(self, connection=None):
        with self.connection.cursor() as cursor:
            cursor.execute("BEGIN")
            try:
                yield self
            except Exception:
                cursor.execute("ROLLBACK")
                raise
            else:
                cursor.execute("COMMIT")

    @contextmanager
    def advisory_session(self, key, connection=None):
        yield True

    def statement(self, query, bindings=(), connection=None):
        with self.connection.cursor() as cursor:
            cursor.execute(query, bindings or None)
            return cursor.rowcount

    def select(self, query, bindings=(), connection=None):
        with self.connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cursor:
            cursor.execute(query, bindings or None)
            return [dict(row) for row in cursor.fetchall()]


def _connect():
    connection = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
    connection.autocommit = True
    return connection


@pytest.fixture
def db():
    connection = _connect()
    real = _RealDB(connection)
    real.statement(
        "CREATE TEMP TABLE schema_operation (LIKE public.schema_operation INCLUDING ALL)"
    )
    yield real
    connection.close()


def _command(monkeypatch, module, cls, db, options, derived=None):
    class _Planner:
        def __init__(self, application):
            pass

        def set_parsed_options(self, parsed):
            pass

        def derive(self):
            return derived, [], []

    monkeypatch.setattr(module, "DB", db)
    if derived is not None:
        monkeypatch.setattr(module, "SchemaPlanCommand", _Planner)
    command = cls.__new__(cls)
    command.application = None
    command.option = options.get
    for level in ("info", "warning", "error", "success"):
        setattr(command, level, lambda *_: None)
    return command


def _applier(monkeypatch, db, derived, **options):
    return _command(
        monkeypatch,
        _APPLY_MODULE,
        _APPLY_MODULE.SchemaApplyCommand,
        db,
        {"connection": "default", "schema": None, "lock_timeout": 5000, **options},
        derived,
    )


def _rollback(monkeypatch, db, **options):
    return _command(
        monkeypatch,
        _ROLLBACK_MODULE,
        _ROLLBACK_MODULE.SchemaRollbackCommand,
        db,
        options,
    )


def _add_column(table, column="closed_at"):
    return Operation(
        kind="add_column",
        table=table,
        key=f"{table}.{column}",
        forward_sql=f'ALTER TABLE "{table}" ADD COLUMN "{column}" TIMESTAMPTZ NULL',
        reverse_sql=f'ALTER TABLE "{table}" DROP COLUMN "{column}"',
        safety=ADDITIVE,
        reason="test",
        restores_data=False,
    )


def _status(db, key):
    return db.select(
        "SELECT status, reversed_at FROM schema_operation WHERE operation_key = %s",
        [key],
    )[0]


def test_a_losing_apply_does_not_mark_the_winners_work_failed(monkeypatch, db):
    """Two runs of one plan (no advisory lock: measured with both queued on one
    table): the loser's "column already exists" overwrote the winner's row with
    ``failed``. The column was there, the ledger said it was not, and rollback
    (which reads ``applied`` rows only) left it behind."""
    db.statement("CREATE TEMP TABLE hunt4_race (id int)")
    operation = _add_column("hunt4_race")
    identifier = plan_id([operation])
    command = _applier(monkeypatch, db, [operation], plan=None)

    def seen_nothing_then_lost_the_race(_plan_id):
        winner = _applier(monkeypatch, db, [operation])
        winner._execute(identifier, operation, 5000)
        return set()

    command._already_applied = seen_nothing_then_lost_the_race

    assert command.handle() == 1
    assert _status(db, operation.key)["status"] == "applied"


def test_a_reapplied_operation_is_not_also_reversed(monkeypatch, db):
    """apply, ``rollback --force``, apply: the row read ``applied`` with the
    reversed_at of the undo it no longer is."""
    db.statement("CREATE TEMP TABLE hunt4_again (id int)")
    operation = _add_column("hunt4_again")
    apply_once = _applier(monkeypatch, db, [operation], plan=None)

    assert apply_once.handle() == 0
    assert _rollback(monkeypatch, db, plan=None, force=True).handle() == 0
    assert _status(db, operation.key)["status"] == "reversed"
    assert _applier(monkeypatch, db, [operation], plan=None).handle() == 0

    row = _status(db, operation.key)
    assert row["status"] == "applied"
    assert row["reversed_at"] is None


def test_a_blocked_statement_stops_at_the_timeout_and_records_no_success(monkeypatch, db):
    """A reader inside a long transaction holds the table: the ADD COLUMN gives
    up at its timeout instead of queueing every writer behind it, on the real
    server rather than a recording fake."""
    table = f"hunt4_blocked_{uuid.uuid4().hex[:8]}"
    db.statement(f'CREATE TABLE "{table}" (id int)')
    reader = _connect()
    try:
        with reader.cursor() as cursor:
            cursor.execute("BEGIN")
            cursor.execute(f'SELECT count(*) FROM "{table}"')
            cursor.fetchall()
            operation = _add_column(table)
            command = _applier(monkeypatch, db, [operation], plan=None, lock_timeout=300)

            started = time.monotonic()
            assert command.handle() == 1
            waited = time.monotonic() - started
            cursor.execute("ROLLBACK")

        assert 0.25 <= waited < 3
        row = _status(db, operation.key)
        assert row["status"] == "failed"
        assert not db.select(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = %s AND column_name = 'closed_at'",
            [table],
        )
    finally:
        reader.close()
        db.statement(f'DROP TABLE IF EXISTS "{table}"')


def test_a_trigger_on_a_new_column_applies_and_rolls_back_in_dependency_order(
    monkeypatch, db
):
    """The ``closed_at`` shape: a column, a function, a trigger ``UPDATE OF`` that
    column. Rollback must take the trigger off before the column (Postgres
    refuses the drop otherwise), and the existing rows must come through."""
    suffix = uuid.uuid4().hex[:8]
    table = f"hunt4_orders_{suffix}"
    function = f"hunt4_stamp_{suffix}"
    trigger = f"hunt4_trg_{suffix}"
    db.statement(f'CREATE TABLE "{table}" (id int, status text)')
    db.statement(
        f"""INSERT INTO "{table}" SELECT g, 'cancelled' FROM generate_series(1, 500) g"""
    )
    create_function = Operation(
        kind="create_index",
        table=table,
        key=f"{table}:{function}",
        forward_sql=(
            f"CREATE OR REPLACE FUNCTION {function}() RETURNS trigger AS $$ "
            "BEGIN NEW.closed_at := clock_timestamp(); RETURN NEW; END; "
            "$$ LANGUAGE plpgsql"
        ),
        reverse_sql=f"DROP FUNCTION IF EXISTS {function}()",
        safety=LOCKING,
        reason="test",
    )
    create_trigger = Operation(
        kind="create_index",
        table=table,
        key=f"{table}:{trigger}",
        forward_sql=(
            f"CREATE OR REPLACE TRIGGER {trigger} BEFORE UPDATE OF status, closed_at "
            f'ON "{table}" FOR EACH ROW EXECUTE FUNCTION {function}()'
        ),
        reverse_sql=f'DROP TRIGGER IF EXISTS {trigger} ON "{table}"',
        safety=LOCKING,
        reason="test",
    )
    operations = [_add_column(table), create_function, create_trigger]
    try:
        assert _applier(monkeypatch, db, operations, plan=None).handle() == 0
        db.statement(f"""UPDATE "{table}" SET status = 'refunded' WHERE id = 1""")
        stamped = db.select(f'SELECT closed_at FROM "{table}" WHERE id = 1')
        assert stamped[0]["closed_at"] is not None
        untouched = db.select(
            f'SELECT count(*) AS n FROM "{table}" WHERE closed_at IS NULL'
        )
        assert untouched[0]["n"] == 499

        # Shape-not-contents: refused until --force names the loss.
        assert _rollback(monkeypatch, db, plan=None, force=False).handle() == 1
        assert _rollback(monkeypatch, db, plan=None, force=True).handle() == 0

        assert not db.select(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = %s AND column_name = 'closed_at'",
            [table],
        )
        assert not db.select("SELECT 1 FROM pg_proc WHERE proname = %s", [function])
        assert not db.select("SELECT 1 FROM pg_trigger WHERE tgname = %s", [trigger])
        assert db.select(f'SELECT count(*) AS n FROM "{table}"')[0]["n"] == 500
    finally:
        db.statement(f'DROP TABLE IF EXISTS "{table}"')
        db.statement(f"DROP FUNCTION IF EXISTS {function}()")
