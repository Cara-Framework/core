"""A plan's artifact after a stop, a repeat and a declared fill of a nullable column.

The red cases of the 500,000-order rehearsal live in
``test_hunt4_evolve_rehearsal_resume.py``; these pin the rest of the same
decisions.
"""

from __future__ import annotations

import importlib
import json
from contextlib import contextmanager

import pytest

from cara.exceptions import SchemaPlanRefused
from cara.schema import (
    ADDITIVE,
    LOCKING,
    LiveSchema,
    Operation,
    as_dict,
    from_dict,
    plan,
    plan_id,
    sort_operations,
)
from cara.schema.Planner import _add_column

_APPLY_MODULE = importlib.import_module("cara.commands.core.SchemaApplyCommand")
_ROLLBACK_MODULE = importlib.import_module("cara.commands.core.SchemaRollbackCommand")


class _Ledger:
    def __init__(self, applied=None, lock_free=True):
        self.applied = applied or {}
        self.statements: list[tuple[str, tuple]] = []
        self.lock_free = lock_free
        self.locks_taken = 0

    @contextmanager
    def transaction(self, connection=None):
        yield self

    @contextmanager
    def advisory_session(self, key, connection=None):
        self.locks_taken += 1
        yield self.lock_free

    def statement(self, query, bindings=(), connection=None):
        self.statements.append((query, tuple(bindings or ())))
        return 1

    def select(self, query, bindings=(), connection=None):
        if "to_regclass" in query:
            return [{"oid": 1}]
        if "operation_key" in query:
            return [{"operation_key": k} for k in self.applied.get(bindings[0], ())]
        return []


def _operation(key, sql, safety=ADDITIVE):
    return Operation(
        kind="add_column",
        table="sales_order",
        key=key,
        forward_sql=sql,
        reverse_sql=f"-- reverse of {key}",
        safety=safety,
        reason="test",
    )


_COLUMN = _operation("sales_order.a", 'ALTER TABLE "sales_order" ADD COLUMN "a" INT NULL')
_OTHER = _operation("sales_order.b", 'ALTER TABLE "sales_order" ADD COLUMN "b" INT NULL')


def _apply(monkeypatch, tmp_path, reviewed, derived, ledger, **options):
    class _Planner:
        def __init__(self, application):
            pass

        def set_parsed_options(self, parsed):
            pass

        def derive(self):
            return derived, [], []

    artifact = tmp_path / "plan.json"
    artifact.write_text(
        json.dumps(
            {
                "plan_id": plan_id(reviewed),
                "operations": [as_dict(op) for op in reviewed],
                "notices": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(_APPLY_MODULE, "SchemaPlanCommand", _Planner)
    monkeypatch.setattr(_APPLY_MODULE, "DB", ledger)
    command = _APPLY_MODULE.SchemaApplyCommand.__new__(_APPLY_MODULE.SchemaApplyCommand)
    command.application = None
    values = {
        "connection": "default",
        "schema": None,
        "plan": str(artifact),
        "lock_timeout": 5000,
        "dry_run": False,
        "allow_destructive": False,
        **options,
    }
    command.option = values.get
    messages: list[str] = []
    for level in ("info", "warning", "error", "success"):
        setattr(command, level, messages.append)
    return command.handle(), messages


# ── the same artifact again ─────────────────────────────────────────────────


def test_a_finished_plan_re_run_from_its_artifact_is_already_applied(
    monkeypatch, tmp_path
):
    reviewed = [_COLUMN, _OTHER]
    ledger = _Ledger(applied={plan_id(reviewed): {_COLUMN.key, _OTHER.key}})

    code, messages = _apply(monkeypatch, tmp_path, reviewed, [], ledger)

    assert code == 0
    assert any("Already applied" in message for message in messages)
    assert not any("MOVED" in message for message in messages)
    assert ledger.statements == []


def test_an_empty_derivation_the_ledger_does_not_explain_is_still_refused(
    monkeypatch, tmp_path
):
    """Empty is what a hand-applied hotfix derives: reviewed, never recorded."""
    reviewed = [_COLUMN]
    ledger = _Ledger()

    code, messages = _apply(monkeypatch, tmp_path, reviewed, [], ledger)

    assert code == 1
    assert any("MOVED" in message for message in messages)


def test_an_empty_derivation_with_part_of_the_plan_recorded_is_refused(
    monkeypatch, tmp_path
):
    reviewed = [_COLUMN, _OTHER]
    ledger = _Ledger(applied={plan_id(reviewed): {_COLUMN.key}})

    code, messages = _apply(monkeypatch, tmp_path, reviewed, [], ledger)

    assert code == 1
    assert any("MOVED" in message for message in messages)


def test_an_operation_that_derives_with_different_sql_is_refused(monkeypatch, tmp_path):
    reviewed = [_COLUMN, _OTHER]
    changed = _operation(_OTHER.key, 'ALTER TABLE "sales_order" ADD COLUMN "b" TEXT NULL')
    ledger = _Ledger(applied={plan_id(reviewed): {_COLUMN.key}})

    code, messages = _apply(monkeypatch, tmp_path, reviewed, [changed], ledger)

    assert code == 1
    assert any("MOVED" in message for message in messages)


def test_a_resumed_plan_records_under_its_reviewed_id(monkeypatch, tmp_path):
    """Under a new id, a default rollback reverses only the second half."""
    reviewed = [_COLUMN, _OTHER]
    ledger = _Ledger(applied={plan_id(reviewed): {_COLUMN.key}})

    code, _ = _apply(monkeypatch, tmp_path, reviewed, [_OTHER], ledger)

    assert code == 0
    recorded = [b for q, b in ledger.statements if q.startswith("INSERT INTO")]
    assert [binding[0] for binding in recorded] == [plan_id(reviewed)]
    assert [binding[1] for binding in recorded] == [_OTHER.key]


# ── one at a time ───────────────────────────────────────────────────────────


def test_apply_refuses_while_another_schema_change_holds_the_lock(monkeypatch, tmp_path):
    ledger = _Ledger(lock_free=False)

    code, messages = _apply(monkeypatch, tmp_path, [_COLUMN], [_COLUMN], ledger)

    assert code == 1
    assert any("Another schema:apply" in message for message in messages)
    assert ledger.statements == []


def test_a_dry_run_takes_no_lock(monkeypatch, tmp_path):
    ledger = _Ledger(lock_free=False)

    code, _ = _apply(monkeypatch, tmp_path, [_COLUMN], [_COLUMN], ledger, dry_run=True)

    assert code == 0
    assert ledger.locks_taken == 0


def _rollback(monkeypatch, ledger, rows, **options):
    ledger.select = lambda query, bindings=(), connection=None: (
        [{"plan_id": "plan123"}] if "ORDER BY id DESC LIMIT 1" in query else rows
    )
    monkeypatch.setattr(_ROLLBACK_MODULE, "DB", ledger)
    command = _ROLLBACK_MODULE.SchemaRollbackCommand.__new__(
        _ROLLBACK_MODULE.SchemaRollbackCommand
    )
    values = {"plan": None, "dry_run": False, "force": True, **options}
    command.option = values.get
    messages: list[str] = []
    for level in ("info", "warning", "error", "success"):
        setattr(command, level, messages.append)
    return command.handle(), messages


def test_rollback_refuses_while_another_schema_change_holds_the_lock(monkeypatch):
    ledger = _Ledger(lock_free=False)
    rows = [{"operation_key": "t.c", "reverse_sql": "DROP", "restores_data": True}]

    code, messages = _rollback(monkeypatch, ledger, rows)

    assert code == 1
    assert any("Another schema:apply" in message for message in messages)
    assert ledger.statements == []


def test_reversing_a_column_also_reverses_the_fill_written_into_it(monkeypatch):
    """Skipped for having no reverse, the fill stayed ``applied``: the next
    apply of the plan re-added the column and skipped the fill as done."""
    ledger = _Ledger()
    rows = [
        {"operation_key": "t.c:backfill", "reverse_sql": None, "restores_data": False},
        {"operation_key": "t.c", "reverse_sql": "DROP", "restores_data": False},
    ]

    code, _ = _rollback(monkeypatch, ledger, rows)

    assert code == 0
    marked = [b for q, b in ledger.statements if q.startswith("UPDATE schema_operation")]
    assert marked == [("plan123", "t.c", "t.c:backfill", "t.c:validate")]


# ── a declared fill of a nullable column ────────────────────────────────────


def _live(columns=()):
    tables = {
        "sales_order": {
            "id": {
                "data_type": "bigint",
                "is_nullable": False,
                "max_length": None,
                "default": None,
            },
            **{
                name: {
                    "data_type": "integer",
                    "is_nullable": True,
                    "max_length": None,
                    "default": None,
                }
                for name in columns
            },
        }
    }
    return LiveSchema(
        tables=tables,
        checks={"sales_order": set()},
        indexes={"sales_order": set()},
        constraint_indexes={"sales_order": set()},
    )


def _model(fill=None, id_type="big_increments", nullable=True):
    params = {"nullable": nullable, "backfill_from": '"seed"'} | (fill or {})
    return {
        "name": "SalesOrder",
        "table": "sales_order",
        "has_fields_method": True,
        "fields": {
            "id": {"type": id_type, "params": {}},
            "filled": {"type": "integer", "params": params},
        },
        "indexes": [],
        "renamed_from": {},
    }


def test_a_nullable_fill_walks_the_key_and_keeps_its_where():
    operations, refusals, _ = plan([_model({"backfill_where": "status = 'x'"})], _live())

    assert refusals == []
    fill = next(op for op in operations if op.kind == "backfill_column")
    assert fill.batch_column == "id"
    assert fill.transactional is False
    assert fill.reverse_sql is None
    assert fill.forward_sql == (
        'UPDATE "sales_order" SET "filled" = "seed" '
        "WHERE \"filled\" IS NULL AND (status = 'x')"
    )


def test_a_table_with_no_key_to_walk_refuses_the_fill():
    operations, refusals, _ = plan([_model(id_type="string")], _live())

    assert not any(op.kind == "backfill_column" for op in operations)
    assert any("batches" in refusal for refusal in refusals)


def test_a_fill_is_not_replanned_for_a_column_nothing_owes():
    operations, refusals, _ = plan([_model()], _live(["filled"]))

    assert operations == []
    assert refusals == []


def test_a_fill_the_ledger_shows_owed_is_derived_on_an_existing_column():
    """An apply stopped between the ADD and the fill: only the ledger knows."""
    owed = frozenset({"sales_order.filled"})

    operations, refusals, _ = plan([_model()], _live(["filled"]), owed)

    assert refusals == []
    assert [op.key for op in operations] == ["sales_order.filled:backfill"]


def test_a_batched_fill_survives_the_artifact_round_trip():
    operation = next(
        op for op in plan([_model()], _live())[0] if op.kind == "backfill_column"
    )

    assert from_dict(as_dict(operation)).batch_column == "id"


def test_a_filled_column_runs_after_the_trigger_that_guards_it():
    """Run first, a fill wrote a close date onto every open order."""
    trigger = _operation(
        "sales_order:stamp",
        "CREATE OR REPLACE TRIGGER stamp BEFORE UPDATE ON sales_order "
        "FOR EACH ROW EXECUTE FUNCTION f()",
        LOCKING,
    )
    trigger = Operation(**{**as_dict(trigger), "notes": (), "kind": "create_index"})
    fill = next(op for op in plan([_model()], _live())[0] if op.kind == "backfill_column")

    ordered = sort_operations([fill, trigger])

    assert [op.key for op in ordered] == [trigger.key, fill.key]


def test_a_new_column_still_precedes_a_function_now_that_both_are_additive():
    column = _operation("sales_order.z", 'ALTER TABLE "sales_order" ADD COLUMN "z" INT')
    function = _operation(
        "sales_order:f",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT z FROM sales_order $$ LANGUAGE sql",
    )
    function = Operation(**{**as_dict(function), "notes": (), "kind": "create_index"})

    assert [op.key for op in sort_operations([function, column])] == [
        column.key,
        function.key,
    ]


@pytest.mark.parametrize(
    "columns",
    [
        {
            "id": {"type": "big_increments", "params": {}, "nullable": False},
            "filled": {
                "type": "integer",
                "nullable": False,
                "params": {"backfill_from": '"seed"', "backfill_where": "seed > 0"},
            },
        }
    ],
)
def test_a_limited_fill_cannot_tighten_to_not_null(columns):
    with pytest.raises(SchemaPlanRefused, match="where="):
        _add_column("sales_order", "filled", columns)


def test_an_index_keeps_its_concurrently_advice_and_a_function_is_lock_free():
    def entry(up):
        return {
            **_model(),
            "fields": {"id": {"type": "big_increments", "params": {}}},
            "indexes": [{"name": "named", "up": up, "down": "-- down"}],
        }

    index, _, _ = plan([entry("CREATE INDEX named ON sales_order (id)")], _live())
    function, _, _ = plan(
        [
            entry(
                "CREATE OR REPLACE FUNCTION named() RETURNS trigger AS $$ "
                "BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql"
            )
        ],
        _live(),
    )

    assert index[0].safety == LOCKING
    assert any("CONCURRENTLY" in note for note in index[0].notes)
    assert function[0].safety == ADDITIVE
