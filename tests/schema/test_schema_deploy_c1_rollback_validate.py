"""``schema:rollback`` must be usable on an ordinary plan.

Found 2026-11-14 (overnight schema-deploy c1): after evolving a 100k clone,
``schema:rollback`` refused BOTH applied plans — "12 operation(s) cannot be
honestly reversed" — and 11 of the 12 were ``validate_check`` /
``validate_foreign_key`` steps. The planner records them with no reverse on
purpose ("validation has nothing to undo — the paired ADD CONSTRAINT's
reverse removes the constraint"), and rollback reads "no reverse" as
"irreversible". Every plan that adds a CHECK or a FOREIGN KEY — nearly all of
them — is therefore un-rollbackable without ``--force``, and ``--force`` also
waves through the reverses that really lose data (a dropped column, a
narrowed width). The "window right after a deploy" the command exists for
offers only the dangerous switch.
"""

from __future__ import annotations

import importlib
from contextlib import contextmanager

_ROLLBACK_MODULE = importlib.import_module("cara.commands.core.SchemaRollbackCommand")


class _LedgerDB:
    """The ledger rows of one plan that added a CHECK and validated it."""

    def __init__(self, rows):
        self.rows = rows
        self.statements: list[str] = []

    @contextmanager
    def transaction(self, connection=None):
        yield self

    @contextmanager
    def advisory_session(self, key, connection=None):
        yield True

    def select(self, query, bindings=(), connection=None):
        return [dict(row) for row in self.rows]

    def statement(self, query, bindings=(), connection=None):
        self.statements.append(query)
        return 1


def test_a_plan_that_added_and_validated_a_check_rolls_back_without_force(monkeypatch):
    rows = [  # newest first, as the ledger hands them back
        {
            "operation_key": "fx_rate:fx_rate_values_check:validate",
            "reverse_sql": None,
            "restores_data": True,
        },
        {
            "operation_key": "fx_rate:fx_rate_values_check",
            "reverse_sql": "ALTER TABLE fx_rate DROP CONSTRAINT IF EXISTS fx_rate_values_check",
            "restores_data": True,
        },
    ]
    db = _LedgerDB(rows)
    monkeypatch.setattr(_ROLLBACK_MODULE, "DB", db)
    cls = _ROLLBACK_MODULE.SchemaRollbackCommand
    command = cls.__new__(cls)
    command.application = None
    options = {"plan": "c1plan", "force": False, "dry_run": False}
    command.option = options.get
    said: list[str] = []
    for level in ("info", "warning", "error", "success"):
        setattr(command, level, said.append)

    code = command._rollback()

    assert code == 0, (
        "rollback refused a plan whose only reverse-less step is a VALIDATE, "
        f"which has nothing to undo: {said}"
    )
    assert any("DROP CONSTRAINT" in statement for statement in db.statements)
