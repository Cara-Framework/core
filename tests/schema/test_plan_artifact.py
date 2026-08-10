"""The plan artifact: reviewable in a pull request, and never stale at apply.

A derived plan's one weakness against a hand-written migration is WHEN it is
reviewed — terminal output during a deploy instead of a file in a pull
request. The artifact closes that, and the staleness gate is what keeps it
from reintroducing the problem it solves: a reviewed file that no longer
describes the database must not run.
"""

from __future__ import annotations

import json

from cara.schema import Operation, as_dict, from_dict, plan_id


def _operation(key="users.phone", forward='ALTER TABLE "users" ADD COLUMN "phone"'):
    return Operation(
        kind="add_column",
        table="users",
        key=key,
        forward_sql=forward,
        reverse_sql='ALTER TABLE "users" DROP COLUMN "phone"',
        safety="additive",
        reason="nullable column declared by the model",
        restores_data=False,
        preflight_sql="SELECT 1",
        preflight_failure="would fail",
        notes=("a note",),
    )


def test_an_operation_round_trips_through_the_artifact():
    """A reviewer reading the file must see what a reviewer reading the
    terminal saw — including the reverse and the preflight, or the artifact is
    a summary pretending to be a plan."""
    original = _operation()
    restored = from_dict(json.loads(json.dumps(as_dict(original))))
    assert restored == original


def test_plan_id_is_content_derived_and_order_sensitive():
    a, b = _operation(), _operation(key="users.tier", forward="ALTER TABLE x")
    assert plan_id([a, b]) == plan_id([a, b])
    assert plan_id([a, b]) != plan_id([b, a])


def test_plan_id_changes_when_a_statement_changes():
    before = plan_id([_operation()])
    after = plan_id(
        [_operation(forward='ALTER TABLE "users" ADD COLUMN "phone" NOT NULL')]
    )
    assert before != after


def test_an_empty_plan_has_a_stable_id_of_its_own():
    """The empty plan needs an id because an artifact approving WORK must not
    match a database that now needs none — the hand-applied-hotfix case."""
    assert plan_id([]) == plan_id([])
    assert plan_id([]) != plan_id([_operation()])
