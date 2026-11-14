"""A reviewed EVOLVE plan must be able to finish on a table that holds rows.

Found 2026-11-14 (overnight schema-deploy c2) evolving a copy of the dev
database (843,530 ``queue_job_delivery`` rows, 1.5 GB), put back to the last
pushed schema, to the current models. The release adds
``queue_job_delivery.lane_key`` ``string(128).default("")`` (NOT NULL).

* ``schema:plan --rehearse`` said "rehearsal clean" (a structure clone has no
  rows), and ``schema:apply`` then died on step 7 of 11:
  ``UPDATE "queue_job_delivery" SET "lane_key" = '' WHERE "lane_key" IS NULL``
  — "canceling statement due to statement timeout". Re-running it dies the
  same way every time. deploy.sh runs the apply AFTER the release boundary
  stopped the API, relay and workers, and after a failed schema phase it keeps
  traffic offline ("deploy a forward fix"): the release is a full outage that
  no re-run can end. The failed UPDATE also wrote 2.2 GB of WAL and left the
  hottest table 1.5 → 2.3 GB of dead rows.

Two roots, one test each:

1. The planner turns a NOT NULL column with a CONSTANT default into
   add-nullable → one unbatched ``UPDATE`` of every row → ``SET NOT NULL``
   (ACCESS EXCLUSIVE full scan). Since PostgreSQL 11 ``ADD COLUMN ... NOT NULL
   DEFAULT <constant>`` is metadata-only: on the same copy it took 179 ms and
   rewrote nothing. The recipe meant to be "safe on a large table" is the one
   that rewrites it.
2. ``schema:apply`` runs under the APPLICATION connection's
   ``statement_timeout`` (30 s in ``config/database.py``, the API's
   user-facing budget), because deploy.sh runs it through the same config
   with only the user swapped. Any honest long step — a batched fill slice
   aside — a ``CREATE INDEX CONCURRENTLY`` on a real table, a ``VALIDATE``
   scan, is cancelled; a cancelled concurrent build leaves an INVALID index.

The Postgres tests need a migrated scratch::

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

from cara.schema import ADDITIVE, LiveSchema, Operation, plan

_APPLY_MODULE = importlib.import_module("cara.commands.core.SchemaApplyCommand")

_INTEGRATION = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and a migrated DB_DATABASE named synk_scratch_*",
)

#: The application's per-statement budget, scaled down with the table: the
#: production config caps every statement at 30 s; these tables are sized so
#: the same steps take several times this budget on a laptop.
_APP_STATEMENT_TIMEOUT_MS = 300
_ROWS = 600_000


def _col(data_type="character varying", nullable=True, max_length=255, default=None):
    return {
        "data_type": data_type,
        "is_nullable": nullable,
        "max_length": max_length,
        "default": default,
    }


def _model(table: str) -> dict:
    return {
        "name": "Delivery",
        "table": table,
        "has_fields_method": True,
        "fields": {
            "id": {"type": "big_increments", "params": {}},
            "lane_key": {"type": "string", "params": {"length": 128, "default": ""}},
        },
        "indexes": [],
        "renamed_from": {},
    }


def _live(table: str) -> LiveSchema:
    return LiveSchema(
        tables={table: {"id": _col("bigint", False, None)}},
        checks={table: set()},
        indexes={table: set()},
        constraint_indexes={table: set()},
    )


def test_a_constant_default_not_null_column_never_rewrites_the_table():
    """The plan for ``lane_key`` on a deployed table must not be one UPDATE of
    every row followed by an ACCESS EXCLUSIVE ``SET NOT NULL`` scan."""
    operations, refusals, _ = plan(
        [_model("queue_job_delivery")], _live("queue_job_delivery")
    )

    assert refusals == []
    unbatched_fills = [
        op.key
        for op in operations
        if op.kind == "backfill_column" and not op.batch_column
    ]
    assert unbatched_fills == [], (
        "a constant default is filled in place by ADD COLUMN ... NOT NULL DEFAULT "
        f"(metadata-only since PostgreSQL 11); the plan rewrites every row: {unbatched_fills}"
    )
    assert "set_not_null" not in [op.kind for op in operations]


def test_an_orphaned_index_drop_records_the_live_definition_as_its_reverse():
    """The same release renames ``queue_job_delivery_tenant_head_idx`` to
    ``..._lane_head_idx``. The drop of the old relay index is planned with
    ``reverse_sql=None`` although the live read already holds its exact
    definition (``index_definitions``) and ``orphaned_indexes`` documents
    "Its reverse fully restores it". Two consequences seen on the dev copy:
    ``schema:rollback`` of the release cannot bring the relay's claim index
    back without ``--force``, and the plan is DESTRUCTIVE, so the documented
    rehearsal on real rows (``restore-drill-logical.sh --evolve``) refuses
    the whole release ("they remove data") and never reaches the backfill
    that times out."""
    table = "queue_job_delivery"
    definition = (
        "USING btree (queue, priority, COALESCE(tenant_id, (0)::bigint), "
        "available_at, created_at) WHERE ((status)::text = 'pending'::text)"
    )
    model = _model(table)
    model["fields"]["lane_key"]["params"]["nullable"] = True
    live = LiveSchema(
        tables={
            table: {
                "id": _col("bigint", False, None),
                "lane_key": _col(max_length=128, default="''::character varying"),
            }
        },
        checks={table: set()},
        indexes={table: {"queue_job_delivery_tenant_head_idx"}},
        constraint_indexes={table: set()},
        index_definitions={table: {"queue_job_delivery_tenant_head_idx": definition}},
    )
    operations, _, _ = plan([model], live)
    drop = next(op for op in operations if op.kind == "drop_index")

    assert drop.reverse_sql is not None, "the old relay index cannot be rolled back"
    assert definition in drop.reverse_sql


# ── on real Postgres ────────────────────────────────────────────────────────


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


def _connect(statement_timeout_ms: int | None = None):
    options = (
        f"-c statement_timeout={statement_timeout_ms}" if statement_timeout_ms else ""
    )
    connection = psycopg2.connect(dbname=os.environ["DB_DATABASE"], options=options)
    connection.autocommit = True
    return connection


@pytest.fixture
def populated():
    """A table with ``_ROWS`` rows, built on an unlimited connection."""
    owner = _RealDB(_connect())
    name = f"sdc2_{uuid.uuid4().hex[:8]}"
    owner.statement(f'CREATE TABLE "{name}" (id bigint primary key, body text)')
    owner.statement(
        f"""INSERT INTO "{name}" SELECT g, md5(g::text) FROM generate_series(1, {_ROWS}) g"""
    )
    owner.statement(f'ANALYZE "{name}"')
    yield name
    owner.statement(f'DROP TABLE IF EXISTS "{name}"')
    owner.connection.close()


@pytest.fixture
def app_db():
    """The connection schema:apply really gets in production: the application's
    config, statement_timeout included."""
    connection = _connect(_APP_STATEMENT_TIMEOUT_MS)
    real = _RealDB(connection)
    real.statement(
        "CREATE TEMP TABLE schema_operation (LIKE public.schema_operation INCLUDING ALL)"
    )
    yield real
    connection.close()


def _applier(monkeypatch, db, operations):
    class _Planner:
        def __init__(self, application):
            pass

        def set_parsed_options(self, parsed):
            pass

        def derive(self):
            return list(operations), [], []

    @contextmanager
    def free(key, connection=None):
        yield True

    db.advisory_session = free
    monkeypatch.setattr(_APPLY_MODULE, "DB", db)
    monkeypatch.setattr(_APPLY_MODULE, "SchemaPlanCommand", _Planner)
    command = _APPLY_MODULE.SchemaApplyCommand.__new__(_APPLY_MODULE.SchemaApplyCommand)
    command.application = None
    values = {"connection": "default", "schema": None, "lock_timeout": 5000}
    command.option = values.get
    errors: list[str] = []
    for level in ("info", "warning", "success"):
        setattr(command, level, lambda *_: None)
    command.error = errors.append
    return command, errors


@_INTEGRATION
def test_the_planned_lane_key_addition_finishes_on_a_populated_table(
    monkeypatch, app_db, populated
):
    model = _model(populated)
    operations, refusals, _ = plan([model], _live(populated))
    assert refusals == []

    command, errors = _applier(monkeypatch, app_db, operations)
    assert command.handle() == 0, errors

    state = app_db.select(
        "SELECT is_nullable, column_default FROM information_schema.columns "
        "WHERE table_name = %s AND column_name = 'lane_key'",
        [populated],
    )
    assert state == [{"is_nullable": "NO", "column_default": "''::character varying"}]
    assert app_db.select(
        f"SELECT count(*) AS n FROM \"{populated}\" WHERE lane_key = ''"
    ) == [{"n": _ROWS}]


@_INTEGRATION
def test_a_concurrent_index_build_is_not_cut_off_by_the_app_statement_budget(
    monkeypatch, app_db, populated
):
    """A CONCURRENTLY build is ADDITIVE and allowed to take its time; under the
    application's statement_timeout it is cancelled and leaves an INVALID index."""
    index = f"{populated}_body_idx"
    operation = Operation(
        kind="create_index",
        table=populated,
        key=f"{populated}:{index}",
        forward_sql=f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {index} ON "{populated}" (body)',
        reverse_sql=f"DROP INDEX CONCURRENTLY IF EXISTS {index}",
        safety=ADDITIVE,
        reason="model declares an index on body",
        transactional=False,
    )

    command, errors = _applier(monkeypatch, app_db, [operation])
    assert command.handle() == 0, errors
    assert app_db.select(
        "SELECT indisvalid AS valid FROM pg_index WHERE indexrelid = %s::regclass",
        [index],
    ) == [{"valid": True}]
