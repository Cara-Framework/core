"""Review of the big-table EVOLVE fix (G03, 2026-11-14): what it left open.

* The orphaned-index drop now records ``CREATE INDEX CONCURRENTLY IF NOT
  EXISTS`` as its reverse, so ``schema:rollback`` rebuilds it — but under the
  API's 30 s ``statement_timeout``, the same budget the fix took away from
  ``schema:apply``. A cut-off rebuild leaves an INVALID index, and the
  re-run's ``IF NOT EXISTS`` is then a no-op that SUCCEEDS: the ledger said
  "reversed" over an index that serves no query (proven on Postgres 18:
  ``NOTICE: relation already exists, skipping``, ``indisvalid = f``).
* An orphaned UNIQUE index was reclassed ``locking`` with the reasoning "its
  cost is query performance". A unique index is an invariant — the thing
  that stops a duplicate order import — so a small table dropped it with no
  approval at all, and once one duplicate lands its rebuild fails.
* The preflight probes (a NULL scan, a duplicate GROUP BY over the whole
  table) still ran under the 30 s API budget, and an unanswerable preflight
  now stops the plan: on a big enough table, on every re-run.

The Postgres test needs a scratch database::

    DB_DATABASE=synk_scratch_<label> CARA_RUN_INTEGRATION=1 pytest <this file>
"""

from __future__ import annotations

import importlib
import os
from contextlib import contextmanager

import pytest

from cara.schema import DESTRUCTIVE, LOCKING, LiveSchema, Operation, plan

_APPLY_MODULE = importlib.import_module("cara.commands.core.SchemaApplyCommand")
_ROLLBACK_MODULE = importlib.import_module("cara.commands.core.SchemaRollbackCommand")

_REBUILD = "CREATE INDEX CONCURRENTLY IF NOT EXISTS t_x_idx ON public.t USING btree (x)"


class _DB:
    """Records statements; answers ``indisvalid`` from a queue of states."""

    def __init__(self, validity=()):
        self.depth = 0
        self.calls: list[tuple[int, str]] = []
        self.validity = list(validity)

    @contextmanager
    def transaction(self, connection=None):
        self.depth += 1
        try:
            yield self
        finally:
            self.depth -= 1

    def statement(self, query, bindings=(), connection=None):
        self.calls.append((self.depth, query))
        return 1

    def select(self, query, bindings=(), connection=None):
        self.calls.append((self.depth, query))
        if "indisvalid" in query:
            state = self.validity.pop(0)
            return [] if state is None else [{"valid": state}]
        return []


def _rollback(monkeypatch, db):
    monkeypatch.setattr(_ROLLBACK_MODULE, "DB", db)
    command = _ROLLBACK_MODULE.SchemaRollbackCommand.__new__(
        _ROLLBACK_MODULE.SchemaRollbackCommand
    )
    command.option = {"statement_timeout": 0}.get
    return command


def _row():
    return {"operation_key": "t:t_x_idx", "reverse_sql": _REBUILD, "restores_data": True}


def test_a_rebuilt_index_drops_an_invalid_leftover_and_runs_under_its_own_budget(
    monkeypatch,
):
    """The second rollback after a cut-off first one: the leftover is INVALID."""
    db = _DB(validity=[False, True])
    command = _rollback(monkeypatch, db)

    command._execute("plan1", _row())

    sent = [sql for _, sql in db.calls if "indisvalid" not in sql]
    assert sent[0] == "DROP INDEX CONCURRENTLY IF EXISTS t_x_idx"
    assert sent[1:4] == ["SET statement_timeout = 0", _REBUILD, "RESET statement_timeout"]
    assert sent[4].startswith("UPDATE schema_operation SET status")


def test_a_rebuild_that_ends_invalid_is_never_marked_reversed(monkeypatch):
    db = _DB(validity=[None, False])
    command = _rollback(monkeypatch, db)

    with pytest.raises(RuntimeError, match="not VALID"):
        command._execute("plan1", _row())

    assert not any(sql.startswith("UPDATE schema_operation") for _, sql in db.calls)
    assert db.calls[-2][1] == "RESET statement_timeout"


def _orphan_plan(definition: str) -> Operation:
    table = "sales_order"
    model = {
        "name": "SalesOrder",
        "table": table,
        "has_fields_method": True,
        "fields": {"id": {"type": "big_increments", "params": {}}},
        "indexes": [],
        "renamed_from": {},
    }
    live = LiveSchema(
        tables={table: {"id": {"data_type": "bigint", "is_nullable": False}}},
        checks={table: set()},
        indexes={table: {"sales_order_dedupe_idx"}},
        constraint_indexes={table: set()},
        index_definitions={table: {"sales_order_dedupe_idx": definition}},
    )
    operations, _, _ = plan([model], live)
    return next(op for op in operations if op.kind == "drop_index")


def test_dropping_an_orphaned_unique_index_stays_an_explicit_decision():
    """The unique index that stops one marketplace order importing twice."""
    drop = _orphan_plan(
        "CREATE UNIQUE INDEX sales_order_dedupe_idx ON public.sales_order "
        "USING btree (channel_id, external_id)"
    )

    assert drop.safety == DESTRUCTIVE
    assert drop.restores_data is False
    assert drop.reverse_sql.startswith("CREATE UNIQUE INDEX CONCURRENTLY")


def test_dropping_an_orphaned_plain_index_is_locking_with_an_exact_reverse():
    drop = _orphan_plan(
        "CREATE INDEX sales_order_dedupe_idx ON public.sales_order USING btree (channel_id)"
    )

    assert drop.safety == LOCKING
    assert drop.restores_data is True


def test_a_preflight_runs_under_the_steps_budget_not_the_api_one(monkeypatch):
    db = _DB()
    monkeypatch.setattr(_APPLY_MODULE, "DB", db)
    command = _APPLY_MODULE.SchemaApplyCommand.__new__(_APPLY_MODULE.SchemaApplyCommand)
    command._statement_budget = 0
    operation = Operation(
        kind="set_not_null",
        table="t",
        key="t.x:not_null",
        forward_sql='ALTER TABLE "t" ALTER COLUMN "x" SET NOT NULL',
        reverse_sql=None,
        safety=LOCKING,
        reason="test",
        preflight_sql='SELECT 1 FROM "t" WHERE "x" IS NULL LIMIT 1',
        preflight_failure="NULLs",
    )

    assert command._preflight(operation) is None
    assert db.calls == [
        (1, "SET LOCAL statement_timeout = 0"),
        (1, operation.preflight_sql),
    ]


# ── on real Postgres ────────────────────────────────────────────────────────

_INTEGRATION = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and DB_DATABASE named synk_scratch_*",
)


class _AutocommitDB:
    """The pooled application connection: autocommit, a 300 ms budget."""

    def __init__(self, connection):
        self.connection = connection

    def statement(self, query, bindings=(), connection=None):
        with self.connection.cursor() as cursor:
            cursor.execute(query, bindings or None)

    def select(self, query, bindings=(), connection=None):
        with self.connection.cursor() as cursor:
            cursor.execute(query, bindings or None)
            names = [column.name for column in cursor.description]
            return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


@_INTEGRATION
def test_a_rollback_rebuild_cut_off_once_finishes_valid_on_the_next_run(monkeypatch):
    import psycopg2

    connection = psycopg2.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USERNAME", "postgres"),
        dbname=os.environ["DB_DATABASE"],
        options="-c statement_timeout=300",
    )
    connection.autocommit = True
    db = _AutocommitDB(connection)
    try:
        db.statement("DROP TABLE IF EXISTS t")
        db.statement("CREATE TABLE t (id bigserial PRIMARY KEY, x int)")
        db.statement("SET statement_timeout = 0")
        db.statement("INSERT INTO t (x) SELECT g FROM generate_series(1, 3000000) g")
        db.statement("RESET statement_timeout")
        # The first rollback, cut off by the API budget: an INVALID leftover.
        with pytest.raises(psycopg2.Error):
            db.statement(_REBUILD)
        monkeypatch.setattr(_ROLLBACK_MODULE, "DB", db)
        command = _ROLLBACK_MODULE.SchemaRollbackCommand.__new__(
            _ROLLBACK_MODULE.SchemaRollbackCommand
        )
        command.option = {"statement_timeout": 0}.get
        monkeypatch.setattr(command, "_mark_reversed", lambda *_: None)

        command._execute("plan1", _row())

        assert command._index_valid("t_x_idx") is True
        assert db.select("SHOW statement_timeout") == [{"statement_timeout": "300ms"}]
    finally:
        db.statement("DROP TABLE IF EXISTS t")
        connection.close()
