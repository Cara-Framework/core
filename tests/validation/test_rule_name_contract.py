"""Validation exposes one canonical snake_case vocabulary."""

from __future__ import annotations

import pytest

from cara.exceptions import RuleNotFoundException
from cara.validation import Validation


def test_rule_registry_contains_only_canonical_multiword_names() -> None:
    validator = Validation()
    registry = validator._Validation__rule_classes

    canonical = {
        "alpha_num",
        "date_format",
        "ends_with",
        "not_in",
        "not_regex",
        "required_if",
        "required_unless",
        "required_with",
        "required_without",
        "starts_with",
    }
    removed_spellings = {
        "alphanum",
        "dateformat",
        "endswith",
        "notin",
        "notregex",
        "requiredif",
        "requiredunless",
        "requiredwith",
        "requiredwithout",
        "startswith",
    }

    assert canonical <= registry.keys()
    assert removed_spellings.isdisjoint(registry)


@pytest.mark.parametrize("rule", ["dateformat:%Y-%m-%d", "requiredif:type,paid"])
def test_removed_rule_spellings_fail_loudly(rule: str) -> None:
    with pytest.raises(RuleNotFoundException):
        Validation.make({"value": "2026-01-01"}, {"value": rule})


def test_canonical_rule_spellings_remain_operational() -> None:
    assert Validation.make(
        {"date": "2026-07-24"},
        {"date": "date_format:%Y-%m-%d"},
    ).passes()
    assert Validation.make(
        {"type": "paid"},
        {"reference": "required_if:type,paid"},
    ).fails()
