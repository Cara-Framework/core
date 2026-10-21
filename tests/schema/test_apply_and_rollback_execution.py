"""How ``schema:apply`` and ``schema:rollback`` execute one statement.

Two promises live here, and both were broken by sending each statement on
its own through an autocommit connection:

* the lock timeout binds. ``SET LOCAL`` outside a transaction block is a
  warning and nothing else, so every DDL ran with no timeout at all and
  queued every writer behind it for as long as it waited;
* a rollback refuses what it cannot honestly undo — including dropping a
  table the plan created, with every row written into it since.
"""

from __future__ import annotations

import importlib
from contextlib import contextmanager

from cara.schema import ADDITIVE, LEDGER_TABLE, LOCKING, Operation

_APPLY_MODULE = importlib.import_module("cara.commands.core.SchemaApplyCommand")
_ROLLBACK_MODULE = importlib.import_module("cara.commands.core.SchemaRollbackCommand")


class _DB:
    """Records every statement with the transaction depth it ran at."""

    def __init__(self, rows=None):
        self.depth = 0
        self.calls: list[tuple[int, str]] = []
        self.rows = rows or []

    @contextmanager
    def transaction(self, connection=None):
        self.depth += 1
        self.calls.append((self.depth, "BEGIN"))
        try:
            yield self
        except Exception:
            self.calls.append((self.depth, "ROLLBACK"))
            raise
        else:
            self.calls.append((self.depth, "COMMIT"))
        finally:
            self.depth -= 1

    def statement(self, query, bindings=(), connection=None):
        self.calls.append((self.depth, query))
        return 1

    def select(self, query, bindings=(), connection=None):
        if "ORDER BY id DESC LIMIT 1" in query:
            return [{"plan_id": "plan123"}]
        return self.rows


def _operation(transactional=True):
    return Operation(
        kind="set_not_null" if transactional else "create_index",
        table="sync_task",
        key="sync_task.x:not_null" if transactional else "sync_task:sync_task_x_index",
        forward_sql=(
            'ALTER TABLE "sync_task" ALTER COLUMN "x" SET NOT NULL'
            if transactional
            else 'CREATE INDEX CONCURRENTLY IF NOT EXISTS sync_task_x_index ON "sync_task" ("x")'
        ),
        reverse_sql='ALTER TABLE "sync_task" ALTER COLUMN "x" DROP NOT NULL',
        safety=LOCKING if transactional else ADDITIVE,
        reason="test",
        transactional=transactional,
    )


def _install(monkeypatch, module, db):
    monkeypatch.setattr(module, "DB", db)


# ── apply ───────────────────────────────────────────────────────────────────


def test_the_lock_timeout_binds_to_the_statement_it_guards(monkeypatch):
    db = _DB()
    _install(monkeypatch, _APPLY_MODULE, db)
    command = _APPLY_MODULE.SchemaApplyCommand.__new__(_APPLY_MODULE.SchemaApplyCommand)

    command._execute("plan123", _operation(), 2500)

    statements = [(depth, sql.split(" (")[0]) for depth, sql in db.calls]
    assert statements[0] == (1, "BEGIN")
    assert statements[1] == (1, "SET LOCAL lock_timeout = '2500ms'")
    assert statements[2] == (1, 'ALTER TABLE "sync_task" ALTER COLUMN "x" SET NOT NULL')
    # The ledger row commits with the change it records, or not at all.
    assert statements[3] == (1, "INSERT INTO schema_operation")
    assert statements[4] == (1, "COMMIT")


def test_a_concurrently_build_runs_outside_any_transaction(monkeypatch):
    """Postgres refuses CONCURRENTLY inside a transaction, and a session-level
    SET would outlive the statement on the pooled connection."""
    db = _DB()
    _install(monkeypatch, _APPLY_MODULE, db)
    command = _APPLY_MODULE.SchemaApplyCommand.__new__(_APPLY_MODULE.SchemaApplyCommand)

    command._execute("plan123", _operation(transactional=False), 2500)

    assert all(depth == 0 for depth, _ in db.calls)
    assert not any("lock_timeout" in sql for _, sql in db.calls)
    assert db.calls[0][1].startswith("CREATE INDEX CONCURRENTLY")
    assert db.calls[1][1].startswith("INSERT INTO schema_operation")


# ── rollback ────────────────────────────────────────────────────────────────


def _rollback(monkeypatch, rows, force=False):
    db = _DB(rows)
    _install(monkeypatch, _ROLLBACK_MODULE, db)
    command = _ROLLBACK_MODULE.SchemaRollbackCommand.__new__(
        _ROLLBACK_MODULE.SchemaRollbackCommand
    )
    options = {"force": force, "plan": None, "dry_run": False}
    command.option = options.get
    messages: list[str] = []
    for level in ("info", "warning", "error", "success"):
        setattr(command, level, messages.append)
    return command, db, messages


def _created_table_row():
    """What the planner now records for a table it creates."""
    from cara.schema import LiveSchema, plan

    model = {
        "table": "order_event",
        "has_fields_method": True,
        "fields": {"id": {"type": "big_increments", "params": {}}},
    }
    live = LiveSchema(tables={}, checks={}, indexes={}, constraint_indexes={})
    (operation,), _, _ = plan([model], live)
    return {
        "operation_key": operation.key,
        "reverse_sql": operation.reverse_sql,
        "restores_data": operation.restores_data,
    }


def test_a_created_table_is_not_dropped_without_force(monkeypatch):
    """Ten minutes of orders in a new table are gone the moment its reverse
    runs; that is shape restored, not contents."""
    command, db, messages = _rollback(monkeypatch, [_created_table_row()])

    assert command.handle() == 1
    assert not any("DROP TABLE" in sql for _, sql in db.calls)
    assert any("SHAPE" in message for message in messages)


def test_the_ledgers_own_creation_is_skipped_even_with_force(monkeypatch):
    rows = [
        {
            "operation_key": "sync_task.x:not_null",
            "reverse_sql": 'ALTER TABLE "sync_task" ALTER COLUMN "x" DROP NOT NULL',
            "restores_data": True,
        },
        {"operation_key": LEDGER_TABLE, "reverse_sql": None, "restores_data": False},
    ]
    command, db, _ = _rollback(monkeypatch, rows, force=True)

    assert command.handle() == 0
    assert not any("DROP TABLE" in sql for _, sql in db.calls)


def test_a_reverse_runs_under_its_lock_timeout_with_its_ledger_update(monkeypatch):
    rows = [
        {
            "operation_key": "sync_task.x:not_null",
            "reverse_sql": 'ALTER TABLE "sync_task" ALTER COLUMN "x" DROP NOT NULL',
            "restores_data": True,
        }
    ]
    command, db, _ = _rollback(monkeypatch, rows)

    assert command.handle() == 0
    assert [depth for depth, _ in db.calls] == [1, 1, 1, 1, 1]
    assert db.calls[1][1].startswith("SET LOCAL lock_timeout")
    assert db.calls[2][1].endswith("DROP NOT NULL")
    assert db.calls[3][1].startswith(f"UPDATE {LEDGER_TABLE} SET status")
    assert db.calls[4][1] == "COMMIT"
