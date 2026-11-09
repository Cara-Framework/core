"""A reviewed artifact resumed from inside a two- or three-step recipe.

The operations of a recipe do not all stop deriving once they ran: a fill still
derives (a nullable column shows nothing), and a ``VALIDATE`` must keep deriving
(the constraint is there, unproven). Both are real plans from
:func:`cara.schema.plan` here, measured first through real craft on a scratch
database: the NOT NULL recipe answered "MOVED" after its fill, and the CHECK
recipe answered "MOVED" and then planned nothing, leaving it ``NOT VALID`` for
good.
"""

from __future__ import annotations

import importlib
import json
from contextlib import contextmanager
from pathlib import Path

from cara.eloquent.migrations.ModelDiscoverer import ModelDiscoverer
from cara.schema import LOCKING, LiveSchema, Operation, as_dict, plan, plan_id
from tests.migrations._fixtures import write_model
from tests.schema.test_rehearsal import _Plan

_APPLY_MODULE = importlib.import_module("cara.commands.core.SchemaApplyCommand")
_PLAN_MODULE = importlib.import_module("cara.commands.core.SchemaPlanCommand")

_TABLE = "widget"
_FILL_KEY = f"{_TABLE}.filled:backfill"
_NOT_NULL_KEY = f"{_TABLE}.filled:not_null"
_CHECK = {"expression": "qty >= 0", "name": "widget_qty_check"}
_CHECK_BODY = "CHECK ((qty >= 0))"
_FOREIGN_KEY = {
    "columns": ["tenant_id"],
    "references": ["id"],
    "on": "tenant",
    "name": "widget_tenant_id_foreign",
}
_FOREIGN_KEY_BODY = "FOREIGN KEY (tenant_id) REFERENCES tenant(id)"


class _Ledger:
    def __init__(self, applied):
        self.applied = applied
        self.statements: list[str] = []
        self.recorded: list[tuple] = []

    @contextmanager
    def transaction(self, connection=None):
        yield self

    @contextmanager
    def advisory_session(self, key, connection=None):
        yield True

    def statement(self, query, bindings=(), connection=None):
        if "schema_operation" in query:
            self.recorded.append(tuple(bindings))
        elif not query.startswith("SET LOCAL"):
            self.statements.append(query)
        return 1

    def select(self, query, bindings=(), connection=None):
        if "to_regclass" in query:
            return [{"oid": 1}]
        if "operation_key" in query:
            return [{"operation_key": k} for k in self.applied.get(bindings[0], ())]
        return []


def _resume(monkeypatch, tmp_path, reviewed, derived, applied_keys):
    class _Planner:
        def __init__(self, application):
            pass

        def set_parsed_options(self, parsed):
            pass

        def derive(self):
            return derived, [], []

    artifact = tmp_path / "plan.json"
    identifier = plan_id(reviewed)
    artifact.write_text(
        json.dumps(
            {
                "plan_id": identifier,
                "operations": [as_dict(op) for op in reviewed],
                "notices": [],
            }
        ),
        encoding="utf-8",
    )
    ledger = _Ledger({identifier: set(applied_keys)})
    monkeypatch.setattr(_APPLY_MODULE, "SchemaPlanCommand", _Planner)
    monkeypatch.setattr(_APPLY_MODULE, "DB", ledger)
    command = _APPLY_MODULE.SchemaApplyCommand.__new__(_APPLY_MODULE.SchemaApplyCommand)
    command.application = None
    command.option = {
        "connection": "default",
        "schema": None,
        "plan": str(artifact),
        "lock_timeout": 5000,
        "dry_run": False,
        "allow_destructive": False,
    }.get
    messages: list[str] = []
    for level in ("info", "warning", "error", "success"):
        setattr(command, level, messages.append)
    return command.handle(), messages, ledger, identifier


# ── the NOT NULL recipe ─────────────────────────────────────────────────────


def _not_null_model():
    return {
        "name": "Widget",
        "table": _TABLE,
        "has_fields_method": True,
        "fields": {
            "id": {"type": "big_increments", "params": {}},
            "filled": {
                "type": "integer",
                "params": {"nullable": False, "backfill_from": '"seed"'},
            },
        },
        "indexes": [],
        "renamed_from": {},
    }


def _column_table(with_filled):
    columns = {"id": {"data_type": "bigint", "is_nullable": False, "max_length": None}}
    if with_filled:
        columns["filled"] = {
            "data_type": "integer",
            "is_nullable": True,
            "max_length": None,
            "default": None,
        }
    return LiveSchema(
        tables={_TABLE: columns},
        checks={_TABLE: set()},
        indexes={_TABLE: set()},
        constraint_indexes={_TABLE: set()},
    )


def _not_null_plans():
    reviewed, refusals, _ = plan([_not_null_model()], _column_table(False))
    derived, more_refusals, _ = plan([_not_null_model()], _column_table(True))
    assert refusals == more_refusals == []
    return reviewed, derived


def test_a_not_null_recipe_stopped_after_its_fill_resumes(monkeypatch, tmp_path):
    """``SET NOT NULL`` is the step a lock timeout or its own preflight stops.
    The fill before it still derives, since a nullable column cannot show it ran,
    so "reviewed minus applied" never equalled the derivation."""
    reviewed, derived = _not_null_plans()
    assert [op.key for op in derived] == [_FILL_KEY, _NOT_NULL_KEY]

    code, messages, ledger, _ = _resume(
        monkeypatch, tmp_path, reviewed, derived, {f"{_TABLE}.filled", _FILL_KEY}
    )

    assert not any("MOVED" in message for message in messages)
    assert code == 0
    assert 'ALTER TABLE "widget" ALTER COLUMN "filled" SET NOT NULL' in ledger.statements


def test_the_resumed_fill_runs_again_before_the_tightening(monkeypatch, tmp_path):
    """Rows written NULL since the fill would stop ``SET NOT NULL`` on every
    re-run; the fill's own ``IS NULL`` makes a second pass safe."""
    reviewed, derived = _not_null_plans()

    _, _, ledger, identifier = _resume(
        monkeypatch, tmp_path, reviewed, derived, {f"{_TABLE}.filled", _FILL_KEY}
    )

    fill = next(op.forward_sql for op in derived if op.key == _FILL_KEY)
    assert ledger.statements == [
        fill,
        'ALTER TABLE "widget" ALTER COLUMN "filled" SET NOT NULL',
    ]
    assert {row[0] for row in ledger.recorded} == {identifier}


def test_a_fill_the_artifact_never_named_is_still_refused(monkeypatch, tmp_path):
    reviewed, derived = _not_null_plans()
    without_the_fill = [op for op in reviewed if op.key != _FILL_KEY]

    code, messages, ledger, _ = _resume(
        monkeypatch,
        tmp_path,
        without_the_fill,
        derived,
        {f"{_TABLE}.filled"},
    )

    assert code == 1
    assert any("MOVED" in message for message in messages)
    assert ledger.statements == []


def test_a_fill_that_derives_with_other_sql_is_refused(monkeypatch, tmp_path):
    """The model's expression changed after the review: a ran fill is exempt
    only as the statement the reviewer read."""
    reviewed, derived = _not_null_plans()
    changed = [
        Operation(**{**as_dict(op), "notes": (), "forward_sql": f"{op.forward_sql} + 1"})
        if op.key == _FILL_KEY
        else op
        for op in derived
    ]

    code, messages, ledger, _ = _resume(
        monkeypatch, tmp_path, reviewed, changed, {f"{_TABLE}.filled", _FILL_KEY}
    )

    assert code == 1
    assert any("MOVED" in message for message in messages)
    assert ledger.statements == []


def test_a_limited_fill_on_an_existing_column_cannot_tighten():
    model = _not_null_model()
    model["fields"]["filled"]["params"]["backfill_where"] = "seed > 0"

    operations, refusals, _ = plan([model], _column_table(True))

    assert operations == []
    assert any("where=" in refusal for refusal in refusals)


def test_a_fill_not_recorded_as_applied_must_still_be_in_the_derivation(
    monkeypatch, tmp_path
):
    reviewed, derived = _not_null_plans()
    only_the_tightening = [op for op in derived if op.key == _NOT_NULL_KEY]

    code, messages, ledger, _ = _resume(
        monkeypatch, tmp_path, reviewed, only_the_tightening, {f"{_TABLE}.filled"}
    )

    assert code == 1
    assert any("MOVED" in message for message in messages)
    assert ledger.statements == []


# ── a constraint left NOT VALID ─────────────────────────────────────────────


def _constraint_model(**extra):
    return {
        "name": "Widget",
        "table": _TABLE,
        "has_fields_method": True,
        "fields": {"id": {"type": "big_increments", "params": {}}},
        "indexes": [],
        "renamed_from": {},
        **extra,
    }


def _constraint_table(name=None, definition=None, declared=None):
    names = {name} if name else set()
    return LiveSchema(
        tables={
            _TABLE: {
                "id": {"data_type": "bigint", "is_nullable": False, "max_length": None}
            },
            "tenant": {
                "id": {"data_type": "bigint", "is_nullable": False, "max_length": None}
            },
        },
        checks={_TABLE: set(names) if "check" in (name or "") else set()},
        indexes={_TABLE: set()},
        constraint_indexes={_TABLE: set()},
        constraints={_TABLE: names},
        constraint_definitions={_TABLE: {name: definition} if name else {}},
        declared_check_definitions={_TABLE: {name: declared} if declared else {}},
    )


def _only_this_model(model, live):
    operations, refusals, _ = plan([model], live)
    assert refusals == []
    return [op for op in operations if op.table == _TABLE]


def test_a_check_left_not_valid_still_owes_its_validate():
    """Stopped between the two steps, e.g. by the VALIDATE's own preflight."""
    live = _constraint_table(
        _CHECK["name"], f"{_CHECK_BODY} NOT VALID", f"{_CHECK_BODY} NOT VALID"
    )

    operations = _only_this_model(_constraint_model(checks=[_CHECK]), live)

    assert [op.key for op in operations] == [f"{_TABLE}:{_CHECK['name']}:validate"]
    assert operations[0].preflight_sql == (
        'SELECT 1 FROM "widget" WHERE NOT (qty >= 0) LIMIT 1'
    )


def test_a_validated_check_derives_nothing():
    live = _constraint_table(_CHECK["name"], _CHECK_BODY, f"{_CHECK_BODY} NOT VALID")

    assert _only_this_model(_constraint_model(checks=[_CHECK]), live) == []


def test_a_replaced_check_is_validated_exactly_once():
    live = _constraint_table(
        _CHECK["name"],
        "CHECK ((qty >= 1)) NOT VALID",
        f"{_CHECK_BODY} NOT VALID",
    )

    keys = [op.key for op in _only_this_model(_constraint_model(checks=[_CHECK]), live)]

    assert keys == [f"{_TABLE}:{_CHECK['name']}", f"{_TABLE}:{_CHECK['name']}:validate"]


def test_a_check_stopped_before_its_validate_resumes_and_runs_it(monkeypatch, tmp_path):
    model = _constraint_model(checks=[_CHECK])
    reviewed = _only_this_model(model, _constraint_table())
    live = _constraint_table(
        _CHECK["name"], f"{_CHECK_BODY} NOT VALID", f"{_CHECK_BODY} NOT VALID"
    )
    derived = _only_this_model(model, live)

    code, messages, ledger, _ = _resume(
        monkeypatch, tmp_path, reviewed, derived, {f"{_TABLE}:{_CHECK['name']}"}
    )

    assert code == 0, messages
    assert ledger.statements == [
        'ALTER TABLE "widget" VALIDATE CONSTRAINT widget_qty_check'
    ]


def test_a_declared_foreign_key_left_not_valid_still_owes_its_validate():
    live = _constraint_table(_FOREIGN_KEY["name"], f"{_FOREIGN_KEY_BODY} NOT VALID")

    operations = _only_this_model(
        _constraint_model(composite_foreign_keys=[_FOREIGN_KEY]), live
    )

    assert [op.key for op in operations] == [f"{_TABLE}:{_FOREIGN_KEY['name']}:validate"]


def test_a_validated_foreign_key_derives_nothing():
    live = _constraint_table(_FOREIGN_KEY["name"], _FOREIGN_KEY_BODY)
    model = _constraint_model(composite_foreign_keys=[_FOREIGN_KEY])

    assert _only_this_model(model, live) == []


def test_a_foreign_key_a_model_declares_not_valid_is_left_as_declared():
    """Named DDL that says NOT VALID meant it: that is the declared shape."""
    live = _constraint_table(_FOREIGN_KEY["name"], f"{_FOREIGN_KEY_BODY} NOT VALID")
    raw = {
        "name": _FOREIGN_KEY["name"],
        "up": (
            f'ALTER TABLE "{_TABLE}" ADD CONSTRAINT {_FOREIGN_KEY["name"]} '
            f"{_FOREIGN_KEY_BODY} NOT VALID"
        ),
    }

    assert _only_this_model(_constraint_model(indexes=[raw]), live) == []


# ── a where the discoverer cannot read ──────────────────────────────────────


def _model_with(where_arguments, tmp_path):
    source = f"""
        from cara.eloquent.schema import Schema

        LIMIT = "status = 'x'"

        class Widget(Model):
            __table__ = "widget"

            @property
            def fields(self):
                return Schema.build(
                    lambda field: (
                        field.big_increments("id"),
                        field.datetime("filled").nullable().backfill_from(
                            "seed", {where_arguments}
                        ),
                    )
                )
    """
    path = write_model(tmp_path, "Widget.py", source)
    info = ModelDiscoverer()._parse_model_file(path)
    return {
        "name": "Widget",
        "table": _TABLE,
        "has_fields_method": True,
        "fields": info["fields"],
        "indexes": [],
        "renamed_from": {},
    }


def test_a_positional_where_limits_the_fill(tmp_path):
    """Read as absent, it would have written every row of the table."""
    operations, refusals, _ = plan(
        [_model_with("\"status = 'x'\"", tmp_path)], _column_table(False)
    )

    assert refusals == []
    fill = next(op for op in operations if op.kind == "backfill_column")
    assert fill.forward_sql.endswith("AND (status = 'x')")


def test_a_where_that_is_not_a_literal_refuses_the_fill(tmp_path):
    """Discovery reads source. A name it cannot resolve must not widen the fill."""
    operations, refusals, _ = plan(
        [_model_with("where=LIMIT", tmp_path)], _column_table(False)
    )

    assert not any(op.kind == "backfill_column" for op in operations)
    assert any("literal" in refusal for refusal in refusals)


# ── what the agent's own tests left open ────────────────────────────────────


def test_a_function_shipped_with_its_trigger_still_locks_the_table():
    entry = {
        "name": "named",
        "up": (
            "CREATE OR REPLACE FUNCTION named() RETURNS trigger AS $$ "
            "BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql; "
            "CREATE TRIGGER named BEFORE UPDATE ON widget "
            "FOR EACH ROW EXECUTE FUNCTION named()"
        ),
        "down": "-- down",
    }

    operations = _only_this_model(_constraint_model(indexes=[entry]), _constraint_table())

    assert [op.safety for op in operations] == [LOCKING]
    assert operations[0].notes == ()


def _rehearsed_artifact(monkeypatch, operations, owed):
    written: dict = {}

    def craft(arguments, name, cwd):
        written.update(json.loads(Path(arguments[2]).read_text(encoding="utf-8")))
        return 0

    rehearsal = _Plan().install(monkeypatch)
    rehearsal.command._owed_backfills = owed
    monkeypatch.setattr(_PLAN_MODULE.Scratch, "run_craft", craft)
    assert rehearsal.rehearse(operations) == 0
    return written


def _nullable_fill_model():
    model = _not_null_model()
    model["fields"]["filled"]["params"]["nullable"] = True
    return model


def test_a_rehearsal_leaves_out_the_fill_only_the_ledger_owes(monkeypatch):
    """A clone has the deployed shape and an empty ledger: it derives no such
    fill, so an artifact naming one would fail as "MOVED"."""
    owed = frozenset({f"{_TABLE}.filled"})
    operations, _, _ = plan([_nullable_fill_model()], _column_table(True), owed)
    assert [op.key for op in operations] == [_FILL_KEY]

    written = _rehearsed_artifact(monkeypatch, operations, owed)

    assert written["operations"] == []
    assert written["plan_id"] == plan_id([])


def test_a_rehearsal_keeps_the_fill_of_a_column_it_adds(monkeypatch):
    operations, _, _ = plan([_nullable_fill_model()], _column_table(False))

    written = _rehearsed_artifact(monkeypatch, operations, frozenset())

    assert [op["key"] for op in written["operations"]] == [
        f"{_TABLE}.filled",
        _FILL_KEY,
    ]
