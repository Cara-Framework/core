"""Numeric bound rules must reject — never crash on — integers past float range.

``json.loads`` turns a body like ``{"quantity": 1000…0}`` (400 digits) into a
Python ``int``; ``float()`` of that raises ``OverflowError``. The bound rules
used to coerce every value with ``float()``, so one oversized number turned
every ``min`` / ``max`` / ``size`` / ``gt`` … door — unauthenticated register
included — into a 500. Python compares ``int`` with ``float`` exactly, so the
rules now compare numbers as they are and only parse strings.
"""

from __future__ import annotations

import pytest

from cara.validation.rules import (
    BetweenRule,
    GteRule,
    GtRule,
    LteRule,
    LtRule,
    MaxRule,
    MinRule,
    SizeRule,
)

HUGE = 10**400


@pytest.mark.parametrize("value", [HUGE, -HUGE])
def test_min_max_size_reject_huge_ints_without_raising(value):
    numeric = {"_rules": ("integer",)}
    assert MaxRule().validate("q", value, {"max": "50", **numeric}) is (value < 0)
    assert MinRule().validate("q", value, {"min": "1", **numeric}) is (value > 0)
    assert SizeRule().validate("q", value, {"size": "5", **numeric}) is False
    assert SizeRule().validate("q", value, {"size": "5"}) is False
    assert BetweenRule().validate("q", value, {"between": "1,50", **numeric}) is False


@pytest.mark.parametrize("value", [HUGE, -HUGE])
def test_comparison_rules_reject_huge_ints_without_raising(value):
    big = value > 0
    assert GtRule().validate("q", value, {"gt": "10"}) is big
    assert GteRule().validate("q", value, {"gte": "10"}) is big
    assert LtRule().validate("q", value, {"lt": "10"}) is (not big)
    assert LteRule().validate("q", value, {"lte": "10"}) is (not big)
    # Cross-field form: the OTHER field holds the huge number.
    assert LtRule().validate("q", 5, {"lt": "o", "_data": {"o": value}}) is big


def test_huge_int_strings_in_numeric_context_are_rejected():
    digits = "9" * 400
    numeric = {"_rules": ("numeric",)}
    assert MaxRule().validate("q", digits, {"max": "50", **numeric}) is False
    assert MinRule().validate("q", "-" + digits, {"min": "1", **numeric}) is False
    assert SizeRule().validate("q", digits, {"size": "5", **numeric}) is False


def test_exact_int_comparison_at_the_float_precision_edge():
    # 2**53 + 1 is not representable as a float; the old float() coercion
    # made it equal to 2**53 and let it past ``max:9007199254740992``.
    edge = 2**53
    assert MaxRule().validate("q", edge + 1, {"max": str(edge)}) is False
    assert MaxRule().validate("q", edge, {"max": str(edge)}) is True
    assert (
        SizeRule().validate("q", edge + 1, {"size": str(edge), "_rules": ("integer",)})
        is False
    )


def test_ordinary_values_keep_their_answers():
    assert MaxRule().validate("q", 50, {"max": "50"}) is True
    assert MaxRule().validate("q", 50.5, {"max": "50"}) is False
    assert MinRule().validate("q", 1, {"min": "1"}) is True
    assert MinRule().validate("q", "abc", {"min": "3"}) is True
    assert SizeRule().validate("q", 5, {"size": "5", "_rules": ("integer",)}) is True
    assert SizeRule().validate("q", "5", {"size": "5", "_rules": ("integer",)}) is True
    assert SizeRule().validate("q", "abcde", {"size": "5"}) is True


def test_max_message_names_the_field_readably():
    rule = MaxRule()
    assert rule.default_message("company_name", {"max": "160", "_value": "x" * 200}) == (
        "The company name may not be greater than 160 characters."
    )
    assert rule.default_message("quantity", {"max": "50", "_value": 99}) == (
        "The quantity may not be greater than 50."
    )
