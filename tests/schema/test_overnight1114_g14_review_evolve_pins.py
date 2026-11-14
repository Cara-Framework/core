"""Pins for the G14 evolve fixes that no test held, and two defects the review found.

Each of these was mutated away on a scratch copy of Cara and the whole schema
suite stayed green, so a later edit could quietly undo it:

* a VALIDATE sorting after the batched backfill that makes its rows valid
  (``ai_credit_entry.period_start`` — the evolve stopped halfway without it);
* a preflight that cannot be answered only because an EARLIER step of the
  same plan adds its column being deferred, not blocking the whole plan;
* ``schema:apply`` refusing a step whose preflight raises, instead of reading
  the error as "no rows";
* a pure ``__renamed_from__`` rename not replacing a CHECK, and a replaced
  CHECK's reverse naming the column as it is called when the reverse runs;
* trigger and function BODY drift becoming one ``replace_routine``.

And the two defects the review found:

1. ``schema:rollback`` asked only ``SET NOT NULL`` and VARCHAR narrowing
   before starting. A widened ``bigint`` or ``numeric(14,2)`` money column
   reverses to ``INTEGER`` / ``NUMERIC(12,2)``, which fails on an order id past
   2^31 or an amount past 10^10 — so ``--force`` sent the rollback in and it
   stopped half reversed, the exact outcome the preflight exists to prevent.
2. A named-DDL entry whose trigger is MISSING while its function body changed
   was planned twice: ``missing_indexes`` creates the entry and
   ``changed_routines`` replaced it, both running the same ``up``. A plain
   ``CREATE TRIGGER`` in it failed the second run on "already exists".
"""

from __future__ import annotations

import importlib
from contextlib import contextmanager

import pytest

from cara.schema import (
    ADDITIVE,
    LOCKING,
    LiveSchema,
    Operation,
    changed_checks,
    changed_routines,
    sort_operations,
)

_PLAN = importlib.import_module("cara.commands.core.SchemaPlanCommand")
_APPLY = importlib.import_module("cara.commands.core.SchemaApplyCommand")
_ROLLBACK = importlib.import_module("cara.commands.core.SchemaRollbackCommand")

_SPEND_CHECK = "entry_type <> 'spend' OR period_start IS NOT NULL"


def _add_period_start() -> Operation:
    return Operation(
        kind="add_column",
        table="ai_credit_entry",
        key="ai_credit_entry.period_start",
        forward_sql='ALTER TABLE "ai_credit_entry" ADD COLUMN "period_start" TIMESTAMPTZ',
        reverse_sql='ALTER TABLE "ai_credit_entry" DROP COLUMN "period_start"',
        safety=ADDITIVE,
        reason="nullable column declared by the model",
    )


def _fill_period_start() -> Operation:
    return Operation(
        kind="backfill_column",
        table="ai_credit_entry",
        key="ai_credit_entry.period_start:backfill",
        forward_sql='UPDATE "ai_credit_entry" SET "period_start" = "occurred_at" '
        "WHERE \"period_start\" IS NULL AND (entry_type = 'spend')",
        reverse_sql=None,
        safety=LOCKING,
        reason="fill the existing spends",
        batch_column="id",
    )


def _validate_spend_check() -> Operation:
    return Operation(
        kind="validate_check",
        table="ai_credit_entry",
        key="ai_credit_entry:ai_credit_entry_spend_period_check:validate",
        forward_sql='ALTER TABLE "ai_credit_entry" VALIDATE CONSTRAINT '
        "ai_credit_entry_spend_period_check",
        reverse_sql=None,
        safety=LOCKING,
        reason="prove the existing rows satisfy the attached CHECK",
        preflight_sql=f'SELECT 1 FROM "ai_credit_entry" WHERE NOT ({_SPEND_CHECK}) LIMIT 1',
        preflight_failure="ai_credit_entry holds spends with no period_start",
    )


def _quiet(command) -> list[str]:
    said: list[str] = []
    for level in ("info", "warning", "error", "success"):
        setattr(command, level, said.append)
    return said


# ── ordering ────────────────────────────────────────────────────────────────


def test_a_validate_runs_after_the_batched_backfill_that_makes_its_rows_valid():
    """Validated first, every existing spend has a NULL period_start and the
    deploy stops on the CHECK the same plan was about to satisfy."""
    ordered = [
        op.key
        for op in sort_operations(
            [_validate_spend_check(), _fill_period_start(), _add_period_start()]
        )
    ]

    assert ordered.index("ai_credit_entry.period_start:backfill") < ordered.index(
        "ai_credit_entry:ai_credit_entry_spend_period_check:validate"
    ), ordered


# ── plan-time preflight ─────────────────────────────────────────────────────


class _ColumnNotThereYet:
    @staticmethod
    def select(sql):
        raise RuntimeError('column "period_start" does not exist')


def _plan_messages(monkeypatch, operations) -> str:
    cls = _PLAN.SchemaPlanCommand
    command = cls.__new__(cls)
    command.option = lambda name: None
    said = _quiet(command)
    command.derive = lambda: (operations, [], [])
    monkeypatch.setattr(_PLAN, "DB", _ColumnNotThereYet)
    command.handle()
    return "\n".join(said)


def test_a_preflight_waiting_on_an_earlier_add_column_is_deferred_not_a_blocker(
    monkeypatch,
):
    """The model's real shape (add the column, then CHECK it) must stay
    plannable: blocking it would make every such release undeployable."""
    text = _plan_messages(monkeypatch, [_add_period_start(), _validate_spend_check()])

    assert "could not be answered" not in text, text
    assert "cannot be asked yet" in text, text


def test_a_preflight_that_fails_with_nothing_reshaping_it_blocks_the_plan(monkeypatch):
    text = _plan_messages(monkeypatch, [_validate_spend_check()])

    assert "could not be answered" in text, text


# ── apply-time preflight ────────────────────────────────────────────────────


def test_apply_reads_a_preflight_that_raises_as_a_blocker_not_as_clear(monkeypatch):
    """At apply every earlier step has run, so an error is no longer "not yet":
    reading it as "no rows" ran a VALIDATE or a UNIQUE build nothing proved."""
    monkeypatch.setattr(_APPLY, "DB", _ColumnNotThereYet)
    cls = _APPLY.SchemaApplyCommand
    command = cls.__new__(cls)

    blocker = command._preflight(_validate_spend_check())

    assert blocker and "could not be answered" in blocker


# ── rollback: every data-dependent reverse is asked first ───────────────────


class _Ledger:
    def __init__(self, rows, offending: str | None):
        self.rows = rows
        self.offending = offending
        self.statements: list[str] = []
        self.asked: list[str] = []

    @contextmanager
    def transaction(self, connection=None):
        yield self

    @contextmanager
    def advisory_session(self, key, connection=None):
        yield True

    def select(self, query, bindings=(), connection=None):
        if "schema_operation" in query:
            return [dict(row) for row in self.rows]
        self.asked.append(query)
        return [{"hit": 1}] if self.offending and self.offending in query else []

    def statement(self, query, bindings=(), connection=None):
        self.statements.append(query)
        return 1


def _rollback(monkeypatch, rows, offending):
    db = _Ledger(rows, offending)
    monkeypatch.setattr(_ROLLBACK, "DB", db)
    cls = _ROLLBACK.SchemaRollbackCommand
    command = cls.__new__(cls)
    command.application = None
    command.option = {"plan": "p1", "force": True, "dry_run": False}.get
    said = _quiet(command)
    return command._rollback(), db, "\n".join(said)


@pytest.mark.parametrize(
    ("reverse", "offending"),
    [
        # sales_order.external_number widened bigint; an id past 2^31 landed.
        (
            'ALTER TABLE "sales_order" ALTER COLUMN "external_number" TYPE INTEGER',
            "NOT BETWEEN -2147483648 AND 2147483647",
        ),
        # a money column widened numeric(12,2) → (14,2); a 10^10 total landed.
        (
            'ALTER TABLE "payout" ALTER COLUMN "amount" TYPE NUMERIC(12,2)',
            ">= 10::numeric ^ 10",
        ),
        (
            'ALTER TABLE "listing" ALTER COLUMN "condition_code" TYPE CHAR(3)',
            "> 3",
        ),
    ],
)
def test_a_narrowing_that_would_fail_on_data_stops_rollback_before_any_reverse(
    monkeypatch, reverse, offending
):
    rows = [  # newest first
        {
            "operation_key": "x.y:type",
            "kind": "widen_column",
            "reverse_sql": reverse,
            "restores_data": False,
        },
        {
            "operation_key": "audit_log:audit_log_x_index",
            "kind": "create_index",
            "reverse_sql": "DROP INDEX CONCURRENTLY IF EXISTS audit_log_x_index",
            "restores_data": True,
        },
    ]

    code, db, text = _rollback(monkeypatch, rows, offending)

    assert code == 1, text
    assert db.statements == [], "a reverse ran before the rollback was refused"


def test_a_narrowing_the_rows_still_fit_rolls_back(monkeypatch):
    rows = [
        {
            "operation_key": "payout.amount:type",
            "kind": "widen_column",
            "reverse_sql": 'ALTER TABLE "payout" ALTER COLUMN "amount" TYPE NUMERIC(12,2)',
            "restores_data": False,
        }
    ]

    code, db, text = _rollback(monkeypatch, rows, offending=None)

    assert code == 0, text
    assert any("NUMERIC(12,2)" in s for s in db.statements)
    assert db.asked, "the narrowing was never asked of the rows"


# ── CHECKs over a renamed column ────────────────────────────────────────────

_ZIP_MODEL = {
    "table": "sales_summary_daily",
    "renamed_from": {"postal_prefix": "zip3"},
    "indexes": [
        {
            "name": "sales_summary_daily_values_check",
            "up": "ALTER TABLE sales_summary_daily ADD CONSTRAINT "
            "sales_summary_daily_values_check CHECK (postal_prefix <> '')",
        }
    ],
}


def _zip_live(declared_body: str) -> LiveSchema:
    name = "sales_summary_daily_values_check"
    return LiveSchema(
        tables={"sales_summary_daily": {"zip3": {"data_type": "character varying"}}},
        checks={"sales_summary_daily": {name}},
        indexes={},
        constraint_indexes={},
        constraints={"sales_summary_daily": {name}},
        constraint_definitions={
            "sales_summary_daily": {name: "CHECK (((zip3)::text <> ''::text))"}
        },
        declared_check_definitions={"sales_summary_daily": {name: declared_body}},
    )


def test_a_pure_rename_does_not_replace_the_check_postgres_already_renamed():
    live = _zip_live("CHECK (((postal_prefix)::text <> ''::text))")

    operations = changed_checks(
        _ZIP_MODEL, "sales_summary_daily", live, {"postal_prefix": "zip3"}
    )

    assert operations == [], [op.key for op in operations]


def test_a_replaced_checks_reverse_names_the_column_as_it_is_called_then():
    """Rollback is newest first: the CHECK's reverse runs while the column is
    still ``postal_prefix``; a body naming ``zip3`` fails and stops it."""
    live = _zip_live("CHECK (((postal_prefix)::text <> 'x'::text))")

    (operation,) = [
        op
        for op in changed_checks(
            _ZIP_MODEL, "sales_summary_daily", live, {"postal_prefix": "zip3"}
        )
        if op.reverse_sql
    ]

    assert "postal_prefix" in operation.reverse_sql
    assert "zip3" not in operation.reverse_sql


# ── trigger and function bodies ─────────────────────────────────────────────

_GUARD = {
    "table": "audit_log",
    "indexes": [
        {
            "name": "audit_log_immutable",
            "up": (
                "CREATE OR REPLACE FUNCTION reject_audit_log_mutation() RETURNS trigger "
                "AS $$ BEGIN RAISE EXCEPTION 'audit_log is append-only'; END $$ "
                "LANGUAGE plpgsql; "
                "CREATE TRIGGER audit_log_immutable BEFORE UPDATE OR DELETE ON "
                "audit_log FOR EACH ROW EXECUTE FUNCTION reject_audit_log_mutation()"
            ),
        }
    ],
}
_OLD_DEFINITION = (
    "CREATE OR REPLACE FUNCTION public.reject_audit_log_mutation()\n RETURNS trigger\n"
    " LANGUAGE plpgsql\nAS $function$ BEGIN RETURN NEW; END $function$\n"
)


def _guard_live(source: str, triggers=("audit_log_immutable",)) -> LiveSchema:
    return LiveSchema(
        tables={"audit_log": {"id": {"data_type": "bigint"}}},
        checks={},
        indexes={},
        constraint_indexes={},
        triggers={"audit_log": set(triggers)},
        functions={"reject_audit_log_mutation"},
        function_definitions={
            "reject_audit_log_mutation": [
                {"source": source, "language": "plpgsql", "definition": _OLD_DEFINITION}
            ]
        },
    )


def test_a_guard_function_whose_body_was_replaced_is_planned_back():
    """``reject_audit_log_mutation`` returning NEW keeps its name and turns
    the append-only audit log writable; it must not read as in sync."""
    (operation,) = changed_routines(
        _GUARD, "audit_log", _guard_live("BEGIN RETURN NEW; END")
    )

    assert operation.kind == "replace_routine"
    assert "RAISE EXCEPTION" in operation.forward_sql
    assert "RETURN NEW" in operation.reverse_sql


def test_a_guard_function_with_the_declared_body_is_not_drift():
    live = _guard_live("BEGIN RAISE EXCEPTION 'audit_log is append-only'; END")

    assert changed_routines(_GUARD, "audit_log", live) == []


def test_an_entry_whose_trigger_is_missing_is_created_once_not_also_replaced():
    live = _guard_live("BEGIN RETURN NEW; END", triggers=())

    assert changed_routines(_GUARD, "audit_log", live) == []


def test_schema_check_reports_a_changed_guard_body_as_drift():
    from cara.commands.core.SchemaCheckCommand import SchemaCheckCommand

    drift = SchemaCheckCommand(application=None)._diff_objects(
        _GUARD, "audit_log", _guard_live("BEGIN RETURN NEW; END")
    )

    assert any("reject_audit_log_mutation" in line for line in drift), drift
