"""An evolve plan on a database that was not reset, part-way and at review.

Found by running a pre-Round-3 schema with 500,000 orders through
``schema:plan --rehearse`` / ``schema:apply`` / ``schema:rollback`` (2026-11-09).
"""

from __future__ import annotations

import importlib
import json
from contextlib import contextmanager

import pytest

from cara.schema import ADDITIVE, LOCKING, LiveSchema, Operation, as_dict, plan, plan_id

_APPLY_MODULE = importlib.import_module("cara.commands.core.SchemaApplyCommand")


class _Ledger:
    """The ledger reads of one apply, and every statement it sends."""

    def __init__(self, applied=None):
        self.applied = applied or {}
        self.statements: list[str] = []

    @contextmanager
    def transaction(self, connection=None):
        yield self

    @contextmanager
    def advisory_session(self, key, connection=None):
        yield True

    def statement(self, query, bindings=(), connection=None):
        self.statements.append(query)
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


_COLUMN = _operation(
    "sales_order.closed_at",
    'ALTER TABLE "sales_order" ADD COLUMN "closed_at" TIMESTAMPTZ NULL',
)
_TRIGGER = _operation(
    "sales_order:stamp",
    "CREATE OR REPLACE TRIGGER stamp BEFORE UPDATE ON sales_order "
    "FOR EACH ROW EXECUTE FUNCTION f()",
    LOCKING,
)
_ELSEWHERE = _operation(
    "listing.other",
    'ALTER TABLE "listing" ADD COLUMN "other" TIMESTAMPTZ NULL',
)


def _apply(monkeypatch, tmp_path, reviewed, derived, ledger):
    """``schema:apply --plan <reviewed>`` where the database now derives ``derived``."""

    class _Planner:
        def __init__(self, application):
            pass

        def set_parsed_options(self, options):
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
    options = {
        "connection": "default",
        "schema": None,
        "plan": str(artifact),
        "lock_timeout": 5000,
        "dry_run": False,
        "allow_destructive": False,
    }
    command.option = options.get
    messages: list[str] = []
    for level in ("info", "warning", "error", "success"):
        setattr(command, level, messages.append)
    return command.handle(), messages


def test_a_plan_stopped_partway_resumes_from_its_reviewed_artifact(monkeypatch, tmp_path):
    """Measured on the 500,000-order rehearsal: a reader held ``sales_order``, the
    second statement hit its 5 s lock timeout and apply said "re-run to continue
    from here". The re-run with the same artifact answered "The database has
    MOVED ... Something changed this schema outside these commands": the applied
    half no longer derives, so the id differs, though the ledger names exactly
    what moved it."""
    reviewed = [_COLUMN, _TRIGGER]
    ledger = _Ledger(applied={plan_id(reviewed): {_COLUMN.key}})

    code, messages = _apply(monkeypatch, tmp_path, reviewed, [_TRIGGER], ledger)

    assert not any("MOVED" in message for message in messages)
    assert code == 0
    assert _TRIGGER.forward_sql in ledger.statements


def test_a_derivation_the_ledger_cannot_explain_is_still_refused(monkeypatch, tmp_path):
    """The staleness gate stays: nothing of the reviewed plan ran, yet part of
    it no longer derives, so something else changed the schema."""
    reviewed = [_COLUMN, _TRIGGER]
    ledger = _Ledger()

    code, messages = _apply(monkeypatch, tmp_path, reviewed, [_TRIGGER], ledger)

    assert code == 1
    assert any("MOVED" in message for message in messages)
    assert ledger.statements == []


def test_work_the_reviewed_plan_never_named_is_refused_after_a_partial_apply(
    monkeypatch, tmp_path
):
    reviewed = [_COLUMN, _TRIGGER]
    ledger = _Ledger(applied={plan_id(reviewed): {_COLUMN.key}})

    code, messages = _apply(monkeypatch, tmp_path, reviewed, [_ELSEWHERE], ledger)

    assert code == 1
    assert any("MOVED" in message for message in messages)
    assert ledger.statements == []


# ── what the plan says about itself ─────────────────────────────────────────


def _live():
    return LiveSchema(
        tables={
            "sales_order": {
                "id": {
                    "data_type": "bigint",
                    "is_nullable": False,
                    "max_length": None,
                    "default": None,
                },
                "status": {
                    "data_type": "character varying",
                    "is_nullable": False,
                    "max_length": 20,
                    "default": None,
                },
            }
        },
        checks={"sales_order": set()},
        indexes={"sales_order": set()},
        constraint_indexes={"sales_order": set()},
    )


def _model(fields=None, indexes=None):
    return {
        "name": "SalesOrder",
        "table": "sales_order",
        "has_fields_method": True,
        "fields": {
            "id": {"type": "big_increments", "params": {}},
            "status": {"type": "string", "params": {"length": 20}},
            **(fields or {}),
        },
        "indexes": indexes or [],
        "renamed_from": {},
    }


@pytest.mark.parametrize(
    "up",
    [
        "CREATE OR REPLACE FUNCTION stamp() RETURNS trigger AS $$ "
        "BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
        "CREATE OR REPLACE TRIGGER stamp_t BEFORE UPDATE OF status ON sales_order "
        "FOR EACH ROW EXECUTE FUNCTION stamp()",
    ],
)
def test_no_concurrent_build_is_advised_for_what_has_no_concurrent_form(up):
    """The plan of the ``closed_at`` trigger told its reviewer to "declare it
    CONCURRENTLY" for a function and a trigger, and Postgres has no such form
    for either; the only way to act on the note is to delete the object."""
    operations, refusals, _ = plan(
        [_model(indexes=[{"name": "stamp", "up": up, "down": "-- down"}])], _live()
    )

    assert refusals == []
    assert [op.key for op in operations] == ["sales_order:stamp"]
    assert not any("CONCURRENTLY" in note for note in operations[0].notes)


def test_a_declared_backfill_on_a_nullable_column_is_not_dropped_silently():
    """``closed_at`` is nullable and its existing closed orders need a value:
    the one declaration a model has for that is accepted and then ignored, so
    the plan carried no backfill and 150,000 closed orders stayed NULL with
    nothing saying so. Either fill them or refuse the declaration."""
    field = {
        "type": "integer",
        "params": {"nullable": True, "backfill_from": '"status_rank"'},
    }
    operations, refusals, _ = plan([_model(fields={"closed_rank": field})], _live())

    planned_a_fill = any(op.kind == "backfill_column" for op in operations)
    assert planned_a_fill or refusals
