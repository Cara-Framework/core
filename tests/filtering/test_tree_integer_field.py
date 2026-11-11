"""A whole-number field refuses, at parse, every value its compiler cannot read.

``TreeField(integer=True)`` checked "is this number whole" with ``Decimal``,
while the compiler (and the in-memory evaluator) bind the value with
``int(value)``. ``Decimal`` reads ``10.0`` and ``1e1`` as the whole number
ten, ``int`` reads neither: the parse accepted them and the compile raised a
bare ``ValueError`` — a 500 for a value the field had just said it took
(filters review, 2026-11-11). The parse now asks the compiler's own question,
so such a value is the field's 422 ("expects whole numbers") with its path.
"""

from __future__ import annotations

import json

import pytest

from cara.exceptions import FilterTreeError
from cara.filtering import FilterTree, TreeField, TreeSchema, compile_tree
from cara.filtering.TreeEvaluator import evaluate_tree


def _schema() -> TreeSchema:
    return TreeSchema(
        "tests.integer_field",
        (TreeField("quantity", "number", column="quantity", integer=True),),
    )


def _parse(operator: str, *values: str) -> FilterTree:
    return FilterTree.parse(
        json.dumps([{"f": "quantity", "o": operator, "v": list(values)}]), _schema()
    )


@pytest.mark.parametrize("value", ["10.0", "1e1", "10.5", "1E2", "0.0", "-3.00"])
def test_a_whole_number_field_refuses_a_value_int_cannot_read(value):
    with pytest.raises(FilterTreeError, match="expects whole numbers") as raised:
        _parse("gt", value)
    assert raised.value.status_code == 422


def test_a_between_refuses_its_second_bound_too():
    with pytest.raises(FilterTreeError, match="expects whole numbers"):
        _parse("between", "1", "1e1")


@pytest.mark.parametrize("value", ["10", "0", "-3", "+7"])
def test_every_value_the_parse_accepts_compiles_and_evaluates(value):
    tree = _parse("gt", value)
    sql, params = compile_tree(tree)
    assert sql == "(quantity > %s)"
    assert params == [int(value)]
    assert isinstance(params[0], int)
    assert evaluate_tree(tree, {"quantity": int(value) + 1}.get) is True


def test_a_decimal_field_still_takes_decimal_spellings():
    schema = TreeSchema(
        "tests.decimal_field", (TreeField("price", "number", column="price"),)
    )
    for value in ("10.0", "1e1", "10.5"):
        tree = FilterTree.parse(
            json.dumps([{"f": "price", "o": "gt", "v": [value]}]), schema
        )
        assert compile_tree(tree) is not None
