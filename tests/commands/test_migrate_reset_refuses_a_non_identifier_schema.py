"""``migrate:reset --schema`` must be a plain identifier, checked up front.

Live defect in ``cara/commands/core/MigrateResetCommand.py``: ``--schema``
was spliced raw into the reset's DO block (``schemaname = '{schema}'`` and
``'DROP TABLE IF EXISTS {schema}.' || ...``). A DO block takes no bind
parameters, so a quote in the value broke out of the literal, and a
mixed-case name was folded onto another schema's name in the DROPs. The
value is now refused with exit 1 before the prompt and before anything
runs, and the SQL uses it only as a literal and through ``%I``.
"""

from __future__ import annotations

import pytest

from cara.commands.core.MigrateResetCommand import MigrateResetCommand
from cara.exceptions import InvalidArgumentException

from ._fixtures import make_command

BAD = [
    "public'; DROP SCHEMA app CASCADE; --",
    "app.tenant",
    "1tenant",
    "with space",
    'quo"te',
    "x" * 64,
]


def _reset(monkeypatch, schema: str):
    command = make_command(MigrateResetCommand, {"confirm": True, "schema": schema})
    executed: list[dict] = []
    errors: list[str] = []
    monkeypatch.setattr(command, "_is_production", lambda: False)
    monkeypatch.setattr(command, "_execute_reset", executed.append)
    monkeypatch.setattr(command, "_flush_job_idempotency_cache", lambda: None)
    monkeypatch.setattr(command, "_display_success_message", lambda: None)
    monkeypatch.setattr(command, "error", lambda message, **_k: errors.append(message))
    return command, executed, errors


@pytest.mark.parametrize("schema", BAD)
def test_a_non_identifier_schema_exits_1_before_anything_runs(
    monkeypatch, schema
) -> None:
    command, executed, errors = _reset(monkeypatch, schema)

    assert command.handle(force=True) == 1
    assert executed == []
    assert any("not a plain identifier" in message for message in errors)


def test_a_plain_mixed_case_schema_proceeds(monkeypatch) -> None:
    command, executed, errors = _reset(monkeypatch, "Tenant_1")

    assert command.handle(force=True) != 1
    assert [config["schema"] for config in executed] == ["Tenant_1"]
    assert errors == []


class _Connection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def query(self, sql, *_args):
        self.statements.append(sql)


@pytest.mark.parametrize("schema", BAD)
def test_the_postgres_reset_itself_refuses_before_any_statement(schema) -> None:
    command = make_command(MigrateResetCommand)
    connection = _Connection()

    with pytest.raises(InvalidArgumentException):
        command._reset_postgresql(connection, schema)
    assert connection.statements == []


def test_the_schema_reaches_the_sql_as_a_literal_and_through_percent_i() -> None:
    command = make_command(MigrateResetCommand)
    connection = _Connection()

    command._reset_postgresql(connection, "Tenant_1")

    (sql,) = connection.statements
    assert "target text := 'Tenant_1';" in sql
    assert "Tenant_1." not in sql, "never spliced bare into a DROP"
    assert "format('DROP TABLE IF EXISTS %I.%I CASCADE', target" in sql
