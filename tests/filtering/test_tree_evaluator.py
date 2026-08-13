"""evaluate_tree — the SQL compiler's twin for computed projections."""

from __future__ import annotations

import datetime

import pytest

from cara.exceptions import FilterTreeCompileError
from cara.filtering import (
    KIND_BOOLEAN,
    KIND_DATE,
    KIND_ENTITY,
    KIND_NUMBER,
    KIND_SELECT,
    KIND_TEXT,
    FilterTree,
    TreeField,
    TreeSchema,
    evaluate_tree,
)

SCHEMA = TreeSchema(
    "evaluator.test",
    (
        TreeField(
            "status",
            KIND_SELECT,
            column="status",
            label="Status",
            options=(("violation", "Violation"), ("unknown", "Unknown")),
        ),
        TreeField(
            "channel",
            KIND_ENTITY,
            column="channel_id",
            label="Channel",
            prefix="CHN",
            source="channels",
        ),
        TreeField("linked", KIND_BOOLEAN, column="linked", label="Linked"),
        TreeField("note", KIND_TEXT, column="note", label="Note", nullable=True),
        TreeField("gap", KIND_NUMBER, column="gap", label="Gap"),
        TreeField("seen", KIND_DATE, column="seen_at", label="Seen"),
    ),
)


def _tree(wire: str) -> FilterTree:
    return FilterTree.parse(wire, SCHEMA)


def _row(**overrides):
    row = {
        "status": "violation",
        "channel": 7,
        "linked": True,
        "note": "below floor",
        "gap": 4.5,
        "seen": datetime.datetime(2026, 8, 10, 12, 0, tzinfo=datetime.UTC),
    }
    row.update(overrides)
    return lambda field: row[field]


def test_empty_tree_matches_everything() -> None:
    assert evaluate_tree(_tree(None), _row()) is True
    assert evaluate_tree(_tree(""), _row()) is True


def test_select_membership_and_negation() -> None:
    tree = _tree('[{"f":"status","o":"in","v":["violation"]}]')
    assert evaluate_tree(tree, _row()) is True
    assert evaluate_tree(tree, _row(status="unknown")) is False
    negated = _tree('[{"f":"status","o":"not_in","v":["violation"]}]')
    assert evaluate_tree(negated, _row(status="unknown")) is True
    assert evaluate_tree(negated, _row()) is False


def test_missing_value_is_honest_under_negation() -> None:
    # SQL's nullable contract, mirrored: "isn't X" includes rows where X
    # was never computed.
    negated = _tree('[{"f":"status","o":"not_in","v":["violation"]}]')
    assert evaluate_tree(negated, _row(status=None)) is True
    positive = _tree('[{"f":"status","o":"in","v":["violation"]}]')
    assert evaluate_tree(positive, _row(status=None)) is False


def test_entity_values_require_resolutions() -> None:
    tree = _tree('[{"f":"channel","o":"in","v":["CHN0123456789ABCDEFGHJKMNPQRS"]}]')
    with pytest.raises(FilterTreeCompileError):
        evaluate_tree(tree, _row())
    resolutions = {"channel": {"CHN0123456789ABCDEFGHJKMNPQRS": 7}}
    assert evaluate_tree(tree, _row(), resolutions=resolutions) is True
    assert evaluate_tree(tree, _row(channel=8), resolutions=resolutions) is False


def test_boolean_and_number_coercions() -> None:
    assert evaluate_tree(_tree('[{"f":"linked","o":"is","v":["true"]}]'), _row()) is True
    assert (
        evaluate_tree(_tree('[{"f":"linked","o":"is","v":["false"]}]'), _row()) is False
    )
    assert evaluate_tree(_tree('[{"f":"gap","o":"gte","v":["4.5"]}]'), _row()) is True
    assert evaluate_tree(_tree('[{"f":"gap","o":"lt","v":["4.5"]}]'), _row()) is False
    assert (
        evaluate_tree(_tree('[{"f":"gap","o":"between","v":["1","5"]}]'), _row()) is True
    )


def test_text_containment_is_case_insensitive() -> None:
    assert (
        evaluate_tree(_tree('[{"f":"note","o":"contains","v":["FLOOR"]}]'), _row())
        is True
    )
    assert (
        evaluate_tree(
            _tree('[{"f":"note","o":"not_contains","v":["floor"]}]'), _row(note=None)
        )
        is True
    )
    assert (
        evaluate_tree(_tree('[{"f":"note","o":"empty","v":[]}]'), _row(note=None)) is True
    )


def test_date_day_boundaries_match_the_compiler() -> None:
    # "before 2026-08-10" excludes that whole day; "after" starts the NEXT day.
    noon = _row()
    assert (
        evaluate_tree(_tree('[{"f":"seen","o":"before","v":["2026-08-10"]}]'), noon)
        is False
    )
    assert (
        evaluate_tree(_tree('[{"f":"seen","o":"before","v":["2026-08-11"]}]'), noon)
        is True
    )
    assert (
        evaluate_tree(_tree('[{"f":"seen","o":"after","v":["2026-08-09"]}]'), noon)
        is True
    )
    assert (
        evaluate_tree(_tree('[{"f":"seen","o":"after","v":["2026-08-10"]}]'), noon)
        is False
    )
    between = _tree('[{"f":"seen","o":"between","v":["2026-08-10","2026-08-10"]}]')
    assert evaluate_tree(between, noon) is True


def test_groups_and_root_connective() -> None:
    grouped = _tree(
        '[{"f":"linked","o":"is","v":["true"]},'
        '{"any":[{"f":"status","o":"in","v":["unknown"]},{"f":"gap","o":"gte","v":["4"]}]}]'
    )
    assert evaluate_tree(grouped, _row()) is True
    assert evaluate_tree(grouped, _row(gap=1, status="violation")) is False
    root_any = _tree(
        '{"any":[{"f":"status","o":"in","v":["unknown"]},{"f":"gap","o":"gte","v":["4"]}]}'
    )
    assert evaluate_tree(root_any, _row(status="violation")) is True
