"""``plan_id`` hashes everything a reviewer approved, and no prose.

Live defect in ``cara/schema/Operation.py``: ``plan_id`` hashed only each
operation's ``key`` and ``forward_sql``. A re-derived plan whose safety
class, reverse statement, preflight, transactional mode, data-restoring
rollback or backfill slicing had changed since review kept the SAME id, so
``schema:apply`` read it as "unchanged" and applied it under an approval
nobody gave. The id is now the hash of ``review_contract(op)``; rewording
the prose (``reason``, ``notes``, ``preflight_failure``) leaves it alone.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from cara.schema.Operation import (
    ADDITIVE,
    DESTRUCTIVE,
    Operation,
    plan_id,
    review_contract,
)

BASE = Operation(
    kind="add_column",
    table="orders",
    key="add_column:orders.note",
    forward_sql='ALTER TABLE "orders" ADD COLUMN "note" TEXT',
    reverse_sql='ALTER TABLE "orders" DROP COLUMN "note"',
    safety=ADDITIVE,
    reason="declared on the model",
    preflight_sql="SELECT 1",
    preflight_failure="column already holds data",
    notes=("first note",),
)


@pytest.mark.parametrize(
    "changes",
    [
        {"safety": DESTRUCTIVE},
        {"reverse_sql": None},
        {"reverse_sql": 'ALTER TABLE "orders" DROP COLUMN "note" CASCADE'},
        {"preflight_sql": None},
        {"preflight_sql": "SELECT count(*) FROM orders"},
        {"transactional": False},
        {"restores_data": False},
        {"batch_column": "id"},
    ],
    ids=lambda changes: next(iter(changes)),
)
def test_a_reviewed_term_changes_the_plan_id(changes) -> None:
    changed = replace(BASE, **changes)

    assert review_contract(changed) != review_contract(BASE)
    assert plan_id([changed]) != plan_id([BASE])


@pytest.mark.parametrize(
    "changes",
    [
        {"reason": "reworded reason"},
        {"notes": ("another note", "and one more")},
        {"preflight_failure": "reworded failure"},
    ],
    ids=lambda changes: next(iter(changes)),
)
def test_prose_does_not_change_the_plan_id(changes) -> None:
    changed = replace(BASE, **changes)

    assert review_contract(changed) == review_contract(BASE)
    assert plan_id([changed]) == plan_id([BASE])


def test_the_forward_statement_and_key_still_count() -> None:
    assert plan_id([replace(BASE, forward_sql="SELECT 2")]) != plan_id([BASE])
    assert plan_id([replace(BASE, key="other")]) != plan_id([BASE])


def test_field_boundaries_cannot_collide() -> None:
    """Each operation is one canonical record: moving text across a field
    boundary is a different plan, not the same concatenated bytes."""
    a = replace(BASE, key="add_column:orders.note", forward_sql="X")
    b = replace(BASE, key="add_column:orders.noteX", forward_sql="")

    assert plan_id([a]) != plan_id([b])
