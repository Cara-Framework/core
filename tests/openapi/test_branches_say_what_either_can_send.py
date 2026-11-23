"""Two branches publish what EITHER can send — never just the first one's.

``unify`` merges the schemas of a ternary's two sides (and of a name assigned
twice, and of one key built in two returns). It started from the first branch
and kept whatever that branch said, so the published contract was narrower
than the code (audit 2026-11-23):

* ``A(x).to_dict() if c else B(y).to_dict()`` was published as ``A``;
* ``A.collection(x) if c else B.collection(y)`` as an array of ``A``;
* a key only the first branch sends was ``required``;
* a key only the second branch sends was missing altogether;
* the first branch's ``format`` described both;
* the second branch's "carries keys this cannot name" was dropped.

Neither product's document changes with the fix — none of these shapes is in
a resource today — which is the moment to close them. A reader of this module
is promised that it "never fabricates a type it cannot see".
"""

from __future__ import annotations

import ast

import pytest

from cara.openapi import ANY_SCHEMA, infer_value_schema, unify

_A = {"$ref": "#/components/schemas/AResource"}
_B = {"$ref": "#/components/schemas/BResource"}


def _schema(expression: str, **options) -> dict:
    return infer_value_schema(ast.parse(expression, mode="eval").body, **options)


# -- references -----------------------------------------------------------


def test_two_different_resources_are_not_the_first_one():
    assert _schema("AResource(x).to_dict() if c else BResource(y).to_dict()") == {}
    assert _schema("AResource(x).to_dict() or BResource(y).to_dict()") == {}


def test_the_same_resource_on_both_sides_is_that_resource():
    assert _schema("AResource(x).to_dict() if c else AResource(y).to_dict()") == _A


def test_the_same_reference_nullable_on_one_side_is_nullable():
    assert unify(_A, {**_A, "nullable": True}) == {**_A, "nullable": True}
    assert unify({**_A, "nullable": True}, _A) == {**_A, "nullable": True}


def test_two_collections_of_different_resources_have_unknown_items():
    schema = _schema("AResource.collection(x) if c else BResource.collection(y)")

    assert schema == {"type": "array", "items": {}}


# -- an empty display agrees with anything ---------------------------------


@pytest.mark.parametrize(
    "expression",
    [
        "AResource.collection(rows) if shown else []",
        "[] if hidden else AResource.collection(rows)",
        "AResource.collection(rows) or []",
        "AResource.collection(rows) if shown else ()",
    ],
)
def test_an_empty_display_keeps_the_other_sides_items(expression):
    # The idiom every redacted list is written in: it sends no item, so it
    # cannot disagree with the items the other side sends.
    assert _schema(expression) == {"type": "array", "items": _A}


def test_an_empty_display_alone_is_still_an_array():
    assert _schema("rows or []") == {"type": "array", "items": {}}
    assert _schema("[] if hidden else rows") == {}


def test_a_non_empty_display_of_unknown_items_does_not_take_the_other_sides():
    schema = _schema("AResource.collection(rows) if shown else [first, second]")

    assert schema == {"type": "array", "items": {}}


# -- objects ---------------------------------------------------------------


def test_a_key_only_one_branch_sends_is_not_required():
    schema = _schema('{"a": 1, "b": 2} if c else {"a": 1}')

    assert sorted(schema["properties"]) == ["a", "b"]
    assert schema["required"] == ["a"]


def test_a_key_only_the_second_branch_sends_is_still_published():
    schema = _schema('{"a": 1} if c else {"a": 1, "z": "late"}')

    assert schema["properties"]["z"] == {"type": "string", "enum": ["late"]}
    assert schema["required"] == ["a"]


def test_branches_with_no_key_in_common_require_none():
    schema = _schema('{"a": 1} if c else {"b": 2}')

    assert sorted(schema["properties"]) == ["a", "b"]
    assert "required" not in schema


def test_an_open_second_branch_opens_the_result():
    schema = _schema('{"a": 1} if c else {"a": 1, **extra}')

    assert schema["additionalProperties"] is True
    assert schema["x-fields-partial"] is True


def test_an_object_only_one_branch_spells_out_vouches_for_no_key():
    call_types = {"opt_dict": {"type": "object", "nullable": True}}

    for expression in (
        '{"a": 1} if c else opt_dict(x)',
        'opt_dict(x) if c else {"a": 1}',
    ):
        assert _schema(expression, call_types=call_types) == {
            "type": "object",
            "nullable": True,
        }


# -- scalars ---------------------------------------------------------------


def test_a_format_only_one_branch_states_does_not_describe_the_other():
    assert _schema("opt_datetime(x) if c else opt_str(y)") == {
        "type": "string",
        "nullable": True,
    }
    assert _schema("opt_str(y) if c else opt_datetime(x)") == {
        "type": "string",
        "nullable": True,
    }


def test_what_both_branches_state_survives():
    dated = {"type": "string", "format": "date-time"}

    assert unify(dated, {**dated, "nullable": True}) == {**dated, "nullable": True}
    assert unify({**dated, "nullable": True}, dated) == {**dated, "nullable": True}


def test_branch_vocabularies_still_accumulate():
    merged = unify(_schema('"a"'), unify(_schema('"b"'), _schema('"c"')))

    assert merged == {"type": "string", "enum": ["a", "b", "c"]}
    assert _schema('"x" if c else opt_str(y)') == {"type": "string", "nullable": True}


def test_different_types_are_unknown():
    assert _schema('"x" if c else 1') == {}
    assert _schema("AResource(x).to_dict() if c else {}") == {}


# -- the unknown schema is nobody's to edit --------------------------------


@pytest.mark.parametrize("expression", ["[first, second]", "[row for row in rows]"])
def test_editing_an_inferred_schema_does_not_retype_every_unknown(expression):
    schema = _schema(expression)

    schema["items"]["type"] = "string"

    assert ANY_SCHEMA == {}
    assert _schema("unknown()") == {}
    assert _schema(expression) == {"type": "array", "items": {}}
