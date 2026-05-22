"""Regression tests for validation bypasses caught during the audit.

Each test pins a specific bypass that was reachable before the fix
shipped alongside this file.
"""

from cara.validation.rules.InRule import InRule
from cara.validation.rules.RequiredRule import RequiredRule


# ── RequiredRule: must reject empty collections ─────────────────────


def test_required_rejects_none():
    assert RequiredRule().validate("name", None, {}) is False


def test_required_rejects_empty_string():
    assert RequiredRule().validate("name", "", {}) is False


def test_required_rejects_whitespace_string():
    assert RequiredRule().validate("name", "   ", {}) is False


def test_required_rejects_empty_list():
    """Previously: ``required|array`` was satisfied by ``[]`` because
    RequiredRule only checked None / "". An attacker submitting an
    empty array bypassed every "must have at least one item" rule
    paired with array typing."""
    assert RequiredRule().validate("items", [], {}) is False


def test_required_rejects_empty_tuple():
    assert RequiredRule().validate("items", (), {}) is False


def test_required_rejects_empty_set():
    assert RequiredRule().validate("items", set(), {}) is False


def test_required_rejects_empty_dict():
    assert RequiredRule().validate("meta", {}, {}) is False


def test_required_accepts_non_empty_collections():
    assert RequiredRule().validate("items", [1], {}) is True
    assert RequiredRule().validate("items", (1,), {}) is True
    assert RequiredRule().validate("meta", {"k": "v"}, {}) is True


def test_required_accepts_zero():
    """0 is a real value — must not be confused with absent."""
    assert RequiredRule().validate("count", 0, {}) is True


def test_required_accepts_false():
    """False is a real value — must not be confused with absent."""
    assert RequiredRule().validate("flag", False, {}) is True


def test_required_accepts_string_zero():
    assert RequiredRule().validate("count", "0", {}) is True


# ── InRule: must reject non-scalar inputs ────────────────────────────


def test_in_rule_accepts_listed_scalar():
    params = {"in": "apple,banana,orange"}
    assert InRule().validate("fruit", "apple", {**params}) is True


def test_in_rule_rejects_unlisted_scalar():
    params = {"in": "apple,banana"}
    assert InRule().validate("fruit", "pear", {**params}) is False


def test_in_rule_rejects_list_input():
    """Previously: ``str(["apple"]) = "['apple']"`` was compared
    against the allowlist — silently failing. Now we reject
    non-scalar inputs explicitly so the caller knows the rule was
    misapplied."""
    params = {"in": "apple,banana"}
    assert InRule().validate("fruit", ["apple"], {**params}) is False


def test_in_rule_rejects_dict_input():
    params = {"in": "apple,banana"}
    assert InRule().validate("fruit", {"value": "apple"}, {**params}) is False


def test_in_rule_rejects_tuple_input():
    params = {"in": "apple,banana"}
    assert InRule().validate("fruit", ("apple",), {**params}) is False


def test_in_rule_rejects_set_input():
    params = {"in": "apple,banana"}
    assert InRule().validate("fruit", {"apple"}, {**params}) is False


def test_in_rule_rejects_none():
    params = {"in": "apple,banana"}
    assert InRule().validate("fruit", None, {**params}) is False


def test_in_rule_coerces_int_to_string():
    """Existing behavior: ``str(1)`` matches ``"1"`` in the
    allowlist — preserved by the fix."""
    params = {"in": "1,2,3"}
    assert InRule().validate("number", 1, {**params}) is True
