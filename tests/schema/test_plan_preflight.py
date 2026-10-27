"""``schema:plan``'s preflight answers about the rows the operation will meet.

A NOT NULL recipe resumed after step 1 (the column added, the apply stopped by
a later failure) re-derives as backfill → SET NOT NULL. Asking the second
step's preflight before the first has run read the NULLs the backfill fills
as a blocker, and the command refused to write the artifact that finishes the
recipe. ``schema:apply`` asks it in plan order; the plan must defer to that.
"""

from __future__ import annotations

import importlib

from cara.schema import LOCKING, Operation

_PLAN_MODULE = importlib.import_module("cara.commands.core.SchemaPlanCommand")


def _backfill(key="users.token_floor_version"):
    return Operation(
        kind="backfill_column",
        table="users",
        key=f"{key}:backfill",
        forward_sql='UPDATE "users" SET "token_floor_version" = 1 '
        'WHERE "token_floor_version" IS NULL',
        reverse_sql=None,
        safety=LOCKING,
        reason="fill the NULL rows with the declared default",
    )


def _set_not_null(key="users.token_floor_version"):
    return Operation(
        kind="set_not_null",
        table="users",
        key=f"{key}:not_null",
        forward_sql='ALTER TABLE "users" ALTER COLUMN "token_floor_version" SET NOT NULL',
        reverse_sql='ALTER TABLE "users" ALTER COLUMN "token_floor_version" DROP NOT NULL',
        safety=LOCKING,
        reason="model tightened the column to NOT NULL",
        preflight_sql='SELECT 1 FROM "users" WHERE "token_floor_version" IS NULL LIMIT 1',
        preflight_failure="users.token_floor_version still has NULL rows",
    )


def _run(monkeypatch, operations):
    from cara.commands.core.SchemaPlanCommand import SchemaPlanCommand

    command = SchemaPlanCommand.__new__(SchemaPlanCommand)
    command.option = lambda name: None
    messages: list[str] = []
    for level in ("info", "warning", "error", "success"):
        setattr(command, level, messages.append)
    command.derive = lambda: (operations, [], [])
    queried: list[str] = []

    class _DB:
        @staticmethod
        def select(sql):
            queried.append(sql)
            return [{"?column?": 1}]  # the column still holds NULL rows

    monkeypatch.setattr(_PLAN_MODULE, "DB", _DB)
    return command.handle(), "\n".join(messages), queried


def test_a_not_null_step_after_its_own_backfill_is_not_blocked(monkeypatch):
    code, text, queried = _run(monkeypatch, [_backfill(), _set_not_null()])
    assert code == 0
    assert queried == []
    assert "preflight runs at apply, after the backfill above" in text


def test_a_not_null_step_without_a_backfill_is_still_asked_now(monkeypatch):
    code, text, queried = _run(monkeypatch, [_set_not_null()])
    assert code == 1
    assert len(queried) == 1
    assert "still has NULL rows" in text


def test_another_columns_backfill_does_not_defer_the_check(monkeypatch):
    code, _, queried = _run(
        monkeypatch, [_backfill("users.other_column"), _set_not_null()]
    )
    assert code == 1
    assert len(queried) == 1
