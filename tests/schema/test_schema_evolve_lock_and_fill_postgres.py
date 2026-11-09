"""The schema-change lock and the batched fill, on real Postgres.

Needs a migrated scratch::

    DB_DATABASE=synk_scratch_<label> CARA_RUN_INTEGRATION=1 pytest <this file>
"""

from __future__ import annotations

import importlib
import os
import uuid
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
import pytest

from cara.eloquent.DatabaseManager import DatabaseManager
from cara.schema import LOCKING, SCHEMA_CHANGE_LOCK, Operation, plan_id

_APPLY_MODULE = importlib.import_module("cara.commands.core.SchemaApplyCommand")

pytestmark = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and a migrated DB_DATABASE named synk_scratch_*",
)


class _RealDB:
    def __init__(self, connection):
        self.connection = connection
        self.statements: list[str] = []

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

    def statement(self, query, bindings=(), connection=None):
        self.statements.append(query)
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


def _manager():
    return DatabaseManager(
        "default",
        {
            "default": {
                "driver": "postgres",
                "host": os.getenv("DB_HOST", "localhost"),
                "database": os.environ["DB_DATABASE"],
                "user": os.getenv("DB_USERNAME") or os.getenv("USER"),
                "password": os.getenv("DB_PASSWORD", ""),
                "port": int(os.getenv("DB_PORT", "5432")),
                "prefix": "",
            }
        },
    )


def _held_by_others(probe):
    with probe.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", [SCHEMA_CHANGE_LOCK])
        taken = cursor.fetchone()[0]
        if taken:
            cursor.execute("SELECT pg_advisory_unlock(%s)", [SCHEMA_CHANGE_LOCK])
        return not taken


def test_the_lock_is_held_across_statements_and_released_at_the_end():
    """On its own connection: a statement between the two must not drop it,
    which is what a lock taken through a pooled ``select`` could not promise."""
    manager, probe = _manager(), _connect()
    try:
        with manager.advisory_session(SCHEMA_CHANGE_LOCK) as held:
            assert held is True
            manager.select("SELECT 1")
            assert _held_by_others(probe)
        assert not _held_by_others(probe)
    finally:
        probe.close()


def test_a_second_session_is_told_the_lock_is_taken():
    first, second = _manager(), _manager()
    with (
        first.advisory_session(SCHEMA_CHANGE_LOCK) as held,
        second.advisory_session(SCHEMA_CHANGE_LOCK) as also,
    ):
        assert held is True
        assert also is False


def _applier(monkeypatch, db, operation, batch_keys=None):
    class _Planner:
        def __init__(self, application):
            pass

        def set_parsed_options(self, parsed):
            pass

        def derive(self):
            return [operation], [], []

    @contextmanager
    def free(key, connection=None):
        yield True

    db.advisory_session = free
    monkeypatch.setattr(_APPLY_MODULE, "DB", db)
    monkeypatch.setattr(_APPLY_MODULE, "SchemaPlanCommand", _Planner)
    if batch_keys is not None:
        monkeypatch.setattr(_APPLY_MODULE, "_BACKFILL_BATCH_KEYS", batch_keys)
    command = _APPLY_MODULE.SchemaApplyCommand.__new__(_APPLY_MODULE.SchemaApplyCommand)
    command.application = None
    values = {"connection": "default", "schema": None, "lock_timeout": 5000}
    command.option = values.get
    for level in ("info", "warning", "error", "success"):
        setattr(command, level, lambda *_: None)
    return command


def _fill(table, where="closed"):
    return Operation(
        kind="backfill_column",
        table=table,
        key=f"{table}.at:backfill",
        forward_sql=f'UPDATE "{table}" SET "at" = "seed" WHERE "at" IS NULL AND ({where})',
        reverse_sql=None,
        safety=LOCKING,
        reason="test",
        restores_data=False,
        transactional=False,
        batch_column="id",
    )


@pytest.fixture
def table(db):
    name = f"se_fill_{uuid.uuid4().hex[:8]}"
    db.statement(
        f'CREATE TABLE "{name}" (id bigint primary key, seed int, at int, closed bool)'
    )
    yield name
    db.statement(f'DROP TABLE IF EXISTS "{name}"')


def test_a_fill_commits_slice_by_slice_over_sparse_keys(monkeypatch, db, table):
    db.statement(
        f"""INSERT INTO "{table}" SELECT g, g, NULL, g % 2 = 0
        FROM generate_series(10, 3000, 7) g"""
    )

    assert _applier(monkeypatch, db, _fill(table), batch_keys=500).handle() == 0

    updates = [s for s in db.statements if s.startswith("UPDATE")]
    assert len(updates) == 6
    assert all('"id" >' in update for update in updates)
    rows = db.select(
        f"""SELECT count(*) FILTER (WHERE at IS NOT NULL) AS filled,
        count(*) FILTER (WHERE NOT closed AND at IS NOT NULL) AS strayed,
        count(*) FILTER (WHERE closed) AS closed FROM "{table}" """
    )[0]
    assert rows["filled"] == rows["closed"]
    assert rows["strayed"] == 0


def test_a_fill_stopped_by_a_held_row_is_recorded_failed_and_finishes_on_re_run(
    monkeypatch, db, table
):
    db.statement(
        f"""INSERT INTO "{table}" SELECT g, g, NULL, true FROM generate_series(1, 2000) g"""
    )
    holder = _connect()
    operation = _fill(table)
    try:
        with holder.cursor() as cursor:
            cursor.execute("BEGIN")
            cursor.execute(f'SELECT 1 FROM "{table}" WHERE id = 1500 FOR UPDATE')
            command = _applier(monkeypatch, db, operation, batch_keys=500)
            command.option = {
                "connection": "default",
                "schema": None,
                "lock_timeout": 200,
            }.get
            assert command.handle() == 1
            filled = db.select(
                f'SELECT count(*) AS n FROM "{table}" WHERE at IS NOT NULL'
            )
            # The slices before the held row stayed committed.
            assert filled[0]["n"] == 1000
            cursor.execute("ROLLBACK")
    finally:
        holder.close()

    assert db.select(
        "SELECT status FROM schema_operation WHERE plan_id = %s",
        [plan_id([operation])],
    ) == [{"status": "failed"}]
    assert _applier(monkeypatch, db, operation, batch_keys=500).handle() == 0
    assert db.select(f'SELECT count(*) AS n FROM "{table}" WHERE at IS NULL') == [
        {"n": 0}
    ]


def test_a_trigger_that_keeps_a_row_null_does_not_stop_the_walk(monkeypatch, db, table):
    """The reason the walk is over keys and not "next rows still NULL"."""
    db.statement(
        f"""INSERT INTO "{table}" SELECT g, g, NULL, false FROM generate_series(1, 40) g"""
    )
    function = f"se_keep_null_{uuid.uuid4().hex[:8]}"
    db.statement(
        f"CREATE FUNCTION {function}() RETURNS trigger AS $$ "
        "BEGIN NEW.at := NULL; RETURN NEW; END; $$ LANGUAGE plpgsql"
    )
    db.statement(
        f'CREATE TRIGGER keep BEFORE UPDATE OF at ON "{table}" '
        f"FOR EACH ROW EXECUTE FUNCTION {function}()"
    )
    try:
        operation = _fill(table, where="true")
        assert _applier(monkeypatch, db, operation, batch_keys=10).handle() == 0
        assert db.select(f'SELECT count(*) AS n FROM "{table}" WHERE at IS NULL') == [
            {"n": 40}
        ]
    finally:
        db.statement(f'DROP TABLE "{table}"')
        db.statement(f"DROP FUNCTION {function}()")
