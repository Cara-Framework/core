from __future__ import annotations

import pytest

from cara.validation import Validation


@pytest.mark.parametrize("value", ["0", "0.00", "1", "12.3456", "9999999999.9999"])
def test_decimal_text_accepts_exact_nonnegative_text(value: str) -> None:
    assert Validation.make({"amount": value}, {"amount": "decimal_text:14,4"}).passes()


@pytest.mark.parametrize(
    "value",
    [12.3, True, -1, "-1", "01", " 1", "1e2", "1.23456", "10000000000.0000"],
)
def test_decimal_text_rejects_lossy_or_out_of_shape_values(value: object) -> None:
    assert Validation.make({"amount": value}, {"amount": "decimal_text:14,4"}).fails()


@pytest.mark.parametrize("rule", ["decimal_text", "decimal_text:x,2", "decimal_text:2,3"])
def test_decimal_text_rejects_invalid_rule_shapes(rule: str) -> None:
    assert Validation.make({"amount": "1.00"}, {"amount": rule}).fails()


def test_named_nested_dict_rules_reject_undeclared_keys() -> None:
    rules = {
        "items": "required|array",
        "items.*": "required|dict",
        "items.*.amount": "required|decimal_text:14,4",
    }
    validator = Validation.make(
        {"items": [{"amount": "1.00", "shadow_amount": "99.00"}]}, rules
    )
    assert validator.fails()
    assert "items.0.shadow_amount" in validator.errors().all()


def test_named_nested_rules_validate_the_concrete_value() -> None:
    rules = {
        "landed_cost": "required|dict",
        "landed_cost.freight": "required|decimal_text:14,4",
    }

    assert Validation.make({"landed_cost": {"freight": "12.50"}}, rules).passes()
    numeric = Validation.make({"landed_cost": {"freight": 12.5}}, rules)
    assert numeric.fails()
    assert "landed_cost.freight" in numeric.errors().all()


def test_validated_parent_owns_nested_children_without_dotted_duplicates() -> None:
    validator = Validation.make(
        {"settings": {"reprice": {"max_price": "25.00"}}},
        {
            "settings": "required|dict",
            "settings.reprice": "required|dict",
            "settings.reprice.max_price": "required|decimal_text:14,4",
        },
    )

    assert validator.passes()
    assert validator.validated() == {"settings": {"reprice": {"max_price": "25.00"}}}


def test_validated_child_normalization_is_overlaid_without_mutating_input() -> None:
    payload = {"settings": {"reprice": {"max_price": ""}}}
    validator = Validation.make(
        payload,
        {
            "settings": "required|dict",
            "settings.reprice": "required|dict",
            "settings.reprice.max_price": "nullable|decimal_text:14,4",
        },
    )

    assert validator.passes()
    assert validator.validated() == {"settings": {"reprice": {"max_price": None}}}
    assert payload == {"settings": {"reprice": {"max_price": ""}}}


def test_validated_dotted_children_reconstruct_when_parent_is_not_declared() -> None:
    validator = Validation.make(
        {"settings": {"reprice": {"max_price": "25.00", "min_price": "10.00"}}},
        {
            "settings.reprice.max_price": "required|decimal_text:14,4",
            "settings.reprice.min_price": "required|decimal_text:14,4",
        },
    )

    assert validator.passes()
    assert validator.validated() == {
        "settings": {"reprice": {"max_price": "25.00", "min_price": "10.00"}}
    }


def test_validated_wildcard_children_reconstruct_source_list_shape() -> None:
    validator = Validation.make(
        {"items": [{"amount": "1.00"}, {"amount": "2.00"}]},
        {"items.*.amount": "required|decimal_text:14,4"},
    )

    assert validator.passes()
    assert validator.validated() == {"items": [{"amount": "1.00"}, {"amount": "2.00"}]}


def test_missing_optional_wildcard_child_stays_optional() -> None:
    rules = {
        "items": "required|array",
        "items.*": "required|dict",
        "items.*.id": "required|string",
        "items.*.note": "string",
    }

    assert Validation.make({"items": [{"id": "item-1"}]}, rules).passes()
