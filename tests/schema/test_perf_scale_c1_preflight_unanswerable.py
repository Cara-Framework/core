"""A preflight the database cannot answer yet is not a clear preflight.

Found 2026-11-14 (overnight perf-scale c1) evolving a day-old 100k-tenant clone:
the model added ``ai_credit_entry.period_start`` (nullable) and a CHECK
``entry_type <> 'spend' OR period_start IS NOT NULL`` in the same change.
``schema:plan`` printed "✓ preflight clear" for the VALIDATE, because its
preflight query names a column that does not exist until step 1 of the same
plan runs, the query raised, and ``_preflight`` swallows every exception as
"no blocker". ``schema:apply`` then stopped after 20 operations with
"ai_credit_entry holds rows violating ai_credit_entry_spend_period_check":
every existing spend row has the new column NULL. A reviewer approved a plan
that the database was certain to refuse halfway.
"""

from __future__ import annotations

import importlib

from cara.schema import ADDITIVE, LOCKING, Operation

_PLAN_MODULE = importlib.import_module("cara.commands.core.SchemaPlanCommand")


def _add_column():
    return Operation(
        kind="add_column",
        table="ai_credit_entry",
        key="ai_credit_entry.period_start",
        forward_sql='ALTER TABLE "ai_credit_entry" ADD COLUMN "period_start" date',
        reverse_sql='ALTER TABLE "ai_credit_entry" DROP COLUMN "period_start"',
        safety=ADDITIVE,
        reason="nullable column declared by the model",
    )


def _validate_check():
    expression = "entry_type <> 'spend' OR period_start IS NOT NULL"
    return Operation(
        kind="validate_check",
        table="ai_credit_entry",
        key="ai_credit_entry:ai_credit_entry_spend_period_check:validate",
        forward_sql='ALTER TABLE "ai_credit_entry" VALIDATE CONSTRAINT '
        "ai_credit_entry_spend_period_check",
        reverse_sql=None,
        safety=LOCKING,
        reason="prove the existing rows satisfy the attached CHECK",
        preflight_sql=f'SELECT 1 FROM "ai_credit_entry" WHERE NOT ({expression}) LIMIT 1',
        preflight_failure="ai_credit_entry holds rows violating "
        "ai_credit_entry_spend_period_check",
    )


def test_an_unanswerable_preflight_is_never_reported_clear(monkeypatch):
    from cara.commands.core.SchemaPlanCommand import SchemaPlanCommand

    command = SchemaPlanCommand.__new__(SchemaPlanCommand)
    command.option = lambda name: None
    messages: list[str] = []
    for level in ("info", "warning", "error", "success"):
        setattr(command, level, messages.append)
    command.derive = lambda: ([_add_column(), _validate_check()], [], [])

    class _DB:
        @staticmethod
        def select(sql):
            # What PostgreSQL answers before step 1 has added the column.
            raise RuntimeError('column "period_start" does not exist')

    monkeypatch.setattr(_PLAN_MODULE, "DB", _DB)
    command.handle()
    text = "\n".join(messages)

    assert "preflight clear" not in text, text
