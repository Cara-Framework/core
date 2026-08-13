"""``FilterTree`` — wire grammar, validation, canonical form.

Pins the parse contract every product leans on: precise path errors,
value canonicalization (set sort/dedupe, between ordering), the
depth-by-grammar rule (no group in group), preserved node order with an
order-independent ``canonical_key``, and entity-id collection across
branches (the authorization walk reads THROUGH groups).
"""

from __future__ import annotations

import json

import pytest

from cara.exceptions import FilterTreeError
from cara.filtering import (
    RAW_LENGTH_CAP,
    FilterTree,
    TreeField,
    TreeGroup,
    TreeSchema,
)

_CHN = "CHN" + "0123456789ABCDEFGHJKMNPQRS"
_CHN2 = "CHN" + "ZZZZZZZZZZZZZZZZZZZZZZZZZZ"


def _schema() -> TreeSchema:
    return TreeSchema(
        "tests.listings",
        (
            TreeField(
                "status",
                "select",
                column="status",
                options=(("active", "Live"), ("draft", "Draft"), ("error", "Error")),
            ),
            TreeField("channel", "entity", column="channel_id", prefix="CHN"),
            TreeField("linked", "boolean", column="product_id"),
            TreeField("title", "text", column="title", nullable=True),
            TreeField("price", "number", column="price", nullable=True),
            TreeField("updated", "date", column="updated_at"),
        ),
    )


def _tree(nodes) -> FilterTree:
    return FilterTree.parse(json.dumps(nodes), _schema())


def test_empty_inputs_parse_to_the_empty_tree():
    schema = _schema()
    for raw in (None, "", "[]"):
        tree = FilterTree.parse(raw, schema)
        assert tree.is_empty
        assert tree.serialize() == ""
        assert tree.canonical_key() == ""


def test_parser_owns_the_raw_payload_ceiling():
    with pytest.raises(FilterTreeError, match="too large"):
        FilterTree.parse(" " * (RAW_LENGTH_CAP + 1), _schema())


def test_in_values_are_deduped_and_sorted():
    tree = _tree([{"f": "status", "o": "in", "v": ["draft", "active", "draft"]}])
    (condition,) = tree.nodes
    assert condition.values == ("active", "draft")
    assert tree.serialize() == '[{"f":"status","o":"in","v":["active","draft"]}]'


def test_between_bounds_sort_numerically_not_lexically():
    tree = _tree([{"f": "price", "o": "between", "v": ["100", "20"]}])
    (condition,) = tree.nodes
    assert condition.values == ("20", "100")


def test_groups_or_join_and_single_child_groups_inline():
    tree = _tree(
        [
            {
                "any": [
                    {"f": "status", "o": "in", "v": ["error"]},
                    {"f": "linked", "o": "is", "v": ["false"]},
                ]
            },
            {"all": [{"f": "status", "o": "not_in", "v": ["draft"]}]},
        ]
    )
    group, inlined = tree.nodes
    assert isinstance(group, TreeGroup) and group.connective == "any"
    assert not isinstance(inlined, TreeGroup)


def test_node_order_is_preserved_but_canonical_key_is_order_free():
    a = _tree(
        [
            {"f": "status", "o": "in", "v": ["active"]},
            {"f": "linked", "o": "is", "v": ["true"]},
        ]
    )
    b = _tree(
        [
            {"f": "linked", "o": "is", "v": ["true"]},
            {"f": "status", "o": "in", "v": ["active"]},
        ]
    )
    assert a.serialize() != b.serialize()
    assert a.canonical_key() == b.canonical_key()


def test_without_root_conditions_drops_only_the_named_root_axis():
    tree = _tree(
        [
            {"f": "status", "o": "in", "v": ["active"]},
            {"f": "linked", "o": "is", "v": ["true"]},
            {
                "any": [
                    {"f": "status", "o": "in", "v": ["error"]},
                    {"f": "linked", "o": "is", "v": ["false"]},
                ]
            },
        ]
    )
    stripped = tree.without_root_conditions("status")
    kept = [
        condition.field for condition in stripped.nodes if hasattr(condition, "field")
    ]
    assert kept == ["linked"]
    # The group is a composite branch — it rides through untouched.
    assert any(not hasattr(node, "field") for node in stripped.nodes)
    # No-op strips return the same tree object.
    assert tree.without_root_conditions("missing") is tree


def test_entity_values_collects_through_groups():
    tree = _tree(
        [
            {"f": "channel", "o": "in", "v": [_CHN2]},
            {
                "any": [
                    {"f": "channel", "o": "not_in", "v": [_CHN]},
                    {"f": "status", "o": "in", "v": ["error"]},
                ]
            },
        ]
    )
    assert tree.entity_values() == {"channel": (_CHN, _CHN2)}


@pytest.mark.parametrize(
    ("nodes", "needle"),
    [
        ([{"f": "bogus", "o": "in", "v": ["x"]}], "unknown field"),
        ([{"f": "status", "o": "gt", "v": ["1"]}], "does not support"),
        ([{"f": "status", "o": "in", "v": ["bogus"]}], "does not accept"),
        ([{"f": "status", "o": "in", "v": []}], "value(s)"),
        ([{"f": "price", "o": "between", "v": ["1"]}], "value(s)"),
        ([{"f": "channel", "o": "in", "v": ["CHN123"]}], "public ids"),
        ([{"f": "linked", "o": "is", "v": ["yes"]}], "true or false"),
        ([{"f": "updated", "o": "before", "v": ["2026-8-1"]}], "ISO dates"),
        ([{"f": "updated", "o": "last_days", "v": ["0"]}], "day count"),
        ([{"f": "price", "o": "gt", "v": ["Infinity"]}], "finite"),
        ([{"any": []}], "at least one condition"),
        ([{"any": [{"any": [{"f": "status", "o": "in", "v": ["draft"]}]}]}], "nest"),
        ([{"f": "status", "o": "in", "v": ["active"], "x": 1}], "unknown key"),
        ("not-json", "not valid JSON"),
        ({"f": "status"}, "one of any/all"),
    ],
)
def test_parse_rejects_with_precise_messages(nodes, needle):
    raw = nodes if isinstance(nodes, str) else json.dumps(nodes)
    with pytest.raises(FilterTreeError) as excinfo:
        FilterTree.parse(raw, _schema())
    assert needle in str(excinfo.value)


def test_structural_caps_are_enforced():
    schema = _schema()
    too_many_roots = [{"f": "status", "o": "in", "v": ["active"]}] * (
        schema.max_root_nodes + 1
    )
    with pytest.raises(FilterTreeError, match="top-level"):
        FilterTree.parse(json.dumps(too_many_roots), schema)
    packed_groups = [
        {
            "any": [
                {"f": "status", "o": "in", "v": ["active"]},
                {"f": "status", "o": "in", "v": ["draft"]},
                {"f": "status", "o": "in", "v": ["error"]},
            ]
        }
    ] * 9
    with pytest.raises(FilterTreeError, match="conditions"):
        FilterTree.parse(json.dumps(packed_groups), schema)


def test_serialize_round_trips_to_an_identical_tree():
    tree = _tree(
        [
            {"f": "title", "o": "contains", "v": [" AirPods "]},
            {
                "any": [
                    {"f": "status", "o": "in", "v": ["error", "draft"]},
                    {"f": "updated", "o": "last_days", "v": ["7"]},
                ]
            },
        ]
    )
    again = FilterTree.parse(tree.serialize(), _schema())
    assert again.serialize() == tree.serialize()
    assert again.canonical_key() == tree.canonical_key()


def test_root_connective_toggles_and_round_trips():
    schema = _schema()
    raw = (
        '{"any":[{"f":"status","o":"in","v":["active"]},'
        '{"all":[{"f":"status","o":"in","v":["error"]},'
        '{"f":"linked","o":"is","v":["false"]}]}]}'
    )
    tree = FilterTree.parse(raw, schema)
    assert tree.connective == "any"
    again = FilterTree.parse(tree.serialize(), schema)
    assert again.connective == "any"
    assert again.serialize() == tree.serialize()
    # Root-AND keeps the historical bare-array spelling byte-stable.
    flat = FilterTree.parse('[{"f":"status","o":"in","v":["active"]}]', schema)
    assert flat.serialize() == '[{"f":"status","o":"in","v":["active"]}]'
    assert flat.canonical_key() != tree.canonical_key()
    with pytest.raises(FilterTreeError, match="one of any/all"):
        FilterTree.parse('{"any":[],"all":[]}', schema)
