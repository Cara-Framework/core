"""A resumed ``schema:apply`` holds what is left to the reviewed contract.

Live defect in ``cara/commands/core/SchemaApplyCommand.py``
``_ledger_explains``: when the ledger had applied part of a reviewed plan,
the operations still to apply were compared with the artifact on their
``forward_sql`` alone. A remaining operation whose reverse, safety class or
preflight had changed since review matched, and apply resumed it under an
approval nobody gave. The comparison now uses ``review_contract``, the same
terms ``plan_id`` hashes, so prose rewording still resumes.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from cara.commands.core.SchemaApplyCommand import SchemaApplyCommand
from cara.schema.Operation import ADDITIVE, DESTRUCTIVE, Operation


def _op(key: str) -> Operation:
    return Operation(
        kind="add_column",
        table="orders",
        key=key,
        forward_sql=f'ALTER TABLE "orders" ADD COLUMN "{key}" TEXT',
        reverse_sql=f'ALTER TABLE "orders" DROP COLUMN "{key}"',
        safety=ADDITIVE,
        reason="declared on the model",
    )


APPLIED, REMAINING = _op("first"), _op("second")


def _explains(derived: list[Operation]) -> bool:
    command = SchemaApplyCommand.__new__(SchemaApplyCommand)
    command._already_applied = lambda _plan: {APPLIED.key}
    return command._ledger_explains("approved-id", [APPLIED, REMAINING], derived)


def test_an_unchanged_remainder_resumes() -> None:
    assert _explains([REMAINING]) is True


def test_reworded_prose_still_resumes() -> None:
    assert _explains([replace(REMAINING, reason="reworded", notes=("n",))]) is True


@pytest.mark.parametrize(
    "changes",
    [
        {"reverse_sql": None},
        {"safety": DESTRUCTIVE},
        {"preflight_sql": "SELECT 1"},
        {"transactional": False},
        {"restores_data": False},
        {"batch_column": "id"},
    ],
    ids=lambda changes: next(iter(changes)),
)
def test_a_remainder_whose_contract_changed_is_not_explained(changes) -> None:
    assert _explains([replace(REMAINING, **changes)]) is False
