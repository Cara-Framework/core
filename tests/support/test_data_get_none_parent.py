"""``data_get`` under a ``None`` parent answers the default.

The live defect: dotty stops at a ``None`` PARENT and returns that ``None`` as
if it were the value, so ``data_get({"a": None}, "a.b", "d")`` answered
``None`` — a JSON ``null`` object in a request body silently replaced every
default read beneath it.
"""

from __future__ import annotations

from cara.support import data_get


def test_a_none_parent_answers_the_default():
    assert data_get({"a": None}, "a.b", "d") == "d"
    assert data_get({"a": {"b": None}}, "a.b.c", 0) == 0


def test_a_none_leaf_is_still_the_value_not_the_default():
    assert data_get({"a": {"b": None}}, "a.b", "d") is None


def test_missing_keys_lists_and_wildcards_are_unchanged():
    payload = {
        "address": {"city": "Izmir"},
        "items": [{"sku": "a"}, {"sku": "b"}],
    }

    assert data_get(payload, "address.city") == "Izmir"
    assert data_get(payload, "address.zip", "none") == "none"
    assert data_get(payload, "missing.deeper", "none") == "none"
    assert data_get(payload, "items.0.sku") == "a"
    assert data_get(payload, "items.*.sku") == ["a", "b"]
