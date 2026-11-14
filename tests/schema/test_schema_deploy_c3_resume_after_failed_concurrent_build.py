"""A plan stopped by a failed ``CREATE INDEX CONCURRENTLY`` must resume from its artifact.

Found 2026-11-14 (overnight schema-deploy c3) evolving a 100k clone
(``synk_stress_scale``) while one ordinary session sat in a 45 s transaction
that had read ``product`` — a different table. ``CREATE INDEX CONCURRENTLY``
waits for EVERY transaction older than its snapshot, not only those on its
table, and it runs under the connection's 10 s ``lock_timeout``
(``config/database.py``), so the 13th operation failed::

    FAILED on ai_credit_entry:ai_credit_entry_tenant_id_entry_type_period_start_index:
    canceling statement due to lock timeout
    12 operation(s) of this plan were applied and are recorded; re-run to
    continue from here once the cause is fixed.

and left the index INVALID. Re-running the same artifact once the session had
committed — exactly what that message, the module docstring ("re-running the
plan drops and rebuilds it") and runbook.md ("A manual schema:apply that
stopped partway ... resumes from the same file") promise — was refused::

    The database has MOVED since lock_plan.json was reviewed.
    approved plan: 1408f29d43c9c7bf   derived now: f180506813407ea4

The INVALID index the failed build left now derives ``rebuild_index``
(``REINDEX INDEX CONCURRENTLY``) instead of the reviewed ``create_index``, so
``_ledger_explains`` cannot account for the difference. The operator is sent
back to re-plan and re-review a schema nobody touched, deploy.sh's next run
derives a NEW plan id, and "one rollback still reverses all of it" stops being
true: the 12 operations stay under the first id, the rest land under another.

The test drives the real command against real Postgres: a first operation
that applies, then a concurrent build cut off by a reader holding an old
snapshot, then the same artifact again once the reader is gone. The planner
is the real one, fed the live table's index state from ``pg_index``.
"""

from __future__ import annotations

import importlib
import json
import os
import uuid
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
import pytest

from cara.schema import LiveSchema, as_dict, plan, plan_id

_APPLY_MODULE = importlib.import_module("cara.commands.core.SchemaApplyCommand")

pytestmark = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and a migrated DB_DATABASE named synk_scratch_*",
)

#: The application connection's lock_timeout (10 s in production), scaled down.
_APP_LOCK_TIMEOUT_MS = 300


class _RealDB:
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


def _connect(options: str = ""):
    connection = psycopg2.connect(dbname=os.environ["DB_DATABASE"], options=options)
    connection.autocommit = True
    return connection


def _model(table: str, with_note: bool) -> dict:
    fields = {
        "id": {"type": "big_increments", "params": {}},
        "tenant_id": {"type": "unsigned_big_integer", "params": {}},
    }
    if with_note:
        fields["note"] = {"type": "string", "params": {"length": 40, "nullable": True}}
    return {
        "name": "Entry",
        "table": table,
        "has_fields_method": True,
        "fields": fields,
        "composite_indexes": [{"columns": ["tenant_id"]}],
        "indexes": [],
        "renamed_from": {},
    }


def _live(db: _RealDB, table: str) -> LiveSchema:
    """The slice of the live schema the planner needs, read from the catalog."""
    columns = {
        row["column_name"]: {
            "data_type": row["data_type"],
            "is_nullable": row["is_nullable"] == "YES",
            "max_length": row["character_maximum_length"],
            "default": None,
        }
        for row in db.select(
            "SELECT column_name, data_type, is_nullable, character_maximum_length "
            "FROM information_schema.columns WHERE table_name = %s",
            [table],
        )
        if row["column_name"] != "id"
    }
    columns["id"] = {
        "data_type": "bigint",
        "is_nullable": False,
        "max_length": None,
        "default": None,
    }
    rows = db.select(
        "SELECT c.relname AS name, i.indisvalid AS valid, "
        "substring(pg_get_indexdef(i.indexrelid) from 'USING .*') AS definition "
        "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
        "WHERE i.indrelid = %s::regclass AND NOT i.indisprimary",
        [table],
    )
    return LiveSchema(
        tables={table: columns},
        checks={table: set()},
        indexes={table: {row["name"] for row in rows}},
        constraint_indexes={table: set()},
        index_definitions={table: {row["name"]: row["definition"] for row in rows}},
        invalid_indexes={table: {row["name"] for row in rows if not row["valid"]}},
    )


@pytest.fixture
def table():
    owner = _RealDB(_connect())
    name = f"sdc3_{uuid.uuid4().hex[:8]}"
    owner.statement(
        f'CREATE TABLE "{name}" (id bigserial primary key, tenant_id bigint NOT NULL)'
    )
    owner.statement(
        f'INSERT INTO "{name}" (tenant_id) SELECT g % 50 FROM generate_series(1, 5000) g'
    )
    yield name
    owner.statement(f'DROP TABLE IF EXISTS "{name}"')
    owner.connection.close()


@pytest.fixture
def app_db():
    """The application's connection: its own lock_timeout, like production."""
    connection = _connect(f"-c lock_timeout={_APP_LOCK_TIMEOUT_MS}")
    real = _RealDB(connection)
    real.statement(
        "CREATE TEMP TABLE schema_operation (LIKE public.schema_operation INCLUDING ALL)"
    )
    yield real
    connection.close()


def _apply(monkeypatch, db, model, artifact, **options) -> tuple[int, list[str]]:
    class _Planner:
        def __init__(self, application):
            pass

        def set_parsed_options(self, parsed):
            pass

        def derive(self):
            operations, refusals, notices = plan([model], _live(db, model["table"]))
            return operations, refusals, notices

    @contextmanager
    def free(key, connection=None):
        yield True

    db.advisory_session = free
    monkeypatch.setattr(_APPLY_MODULE, "DB", db)
    monkeypatch.setattr(_APPLY_MODULE, "SchemaPlanCommand", _Planner)
    command = _APPLY_MODULE.SchemaApplyCommand.__new__(_APPLY_MODULE.SchemaApplyCommand)
    command.application = None
    values = {"connection": "default", "schema": None, "plan": str(artifact), **options}
    command.option = values.get
    errors: list[str] = []
    for level in ("info", "warning", "success"):
        setattr(command, level, lambda *_: None)
    command.error = errors.append
    return command.handle(), errors


def test_a_plan_cut_off_by_a_failed_concurrent_build_resumes_from_its_artifact(
    monkeypatch, tmp_path, app_db, table
):
    model = _model(table, with_note=True)
    reviewed, refusals, _ = plan([model], _live(app_db, table))
    assert refusals == []
    assert [op.kind for op in reviewed] == ["add_column", "create_index"]
    artifact = tmp_path / "plan.json"
    artifact.write_text(
        json.dumps(
            {"plan_id": plan_id(reviewed), "operations": [as_dict(op) for op in reviewed]}
        )
    )

    # Any old snapshot anywhere in the database stops a concurrent build.
    reader = _connect()
    with reader.cursor() as cursor:
        cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        cursor.execute("SELECT count(*) FROM pg_class")
    try:
        # Cut off by its statement budget (a concurrent build waits out the
        # reader with no lock timeout since the fix): it leaves the index
        # INVALID exactly as the lock timeout used to.
        first, errors = _apply(
            monkeypatch, app_db, model, artifact, statement_timeout=_APP_LOCK_TIMEOUT_MS
        )
    finally:
        with reader.cursor() as cursor:
            cursor.execute("COMMIT")
        reader.close()
    assert first == 1, "the build was expected to be cut off by the open reader"
    assert any("re-run to continue" in error for error in errors), errors

    # What the message says to do, with nothing else changed.
    second, errors = _apply(monkeypatch, app_db, model, artifact)

    assert second == 0, f"the same artifact did not resume: {errors}"
    assert app_db.select(
        "SELECT i.indisvalid AS valid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
        "WHERE c.relname = %s",
        [f"{table}_tenant_id_index"],
    ) == [{"valid": True}]
    plans = app_db.select(
        "SELECT DISTINCT plan_id FROM schema_operation WHERE status = 'applied'"
    )
    assert plans == [{"plan_id": plan_id(reviewed)}], "the plan was split across ids"


def test_a_concurrent_build_outwaits_an_ordinary_transaction_elsewhere(
    monkeypatch, tmp_path, app_db, table
):
    """A concurrent build blocks no writer: waiting out a transaction that
    is merely older than it costs nothing. Under the application's
    lock_timeout it fails instead whenever ANY session in the database has
    held a snapshot longer than that — 10 s in production, where a 30 s
    statement budget and a 60 s idle-in-transaction limit are normal — so a
    deploy's index build fails on ordinary traffic and leaves an INVALID
    index (the first test). Here the reader, on another table, commits after
    about three of the app's lock timeouts."""
    import threading

    model = _model(table, with_note=False)
    reviewed, _, _ = plan([model], _live(app_db, table))
    artifact = tmp_path / "plan.json"
    artifact.write_text(
        json.dumps(
            {"plan_id": plan_id(reviewed), "operations": [as_dict(op) for op in reviewed]}
        )
    )
    reader = _connect()
    with reader.cursor() as cursor:
        cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        cursor.execute("SELECT count(*) FROM pg_class")

    def finish():
        with reader.cursor() as cursor:
            cursor.execute("COMMIT")

    timer = threading.Timer(3 * _APP_LOCK_TIMEOUT_MS / 1000, finish)
    timer.start()
    try:
        status, errors = _apply(monkeypatch, app_db, model, artifact)
    finally:
        timer.join()
        reader.close()

    assert status == 0, f"the build gave up behind an unrelated transaction: {errors}"
