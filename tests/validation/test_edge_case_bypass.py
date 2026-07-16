"""Regression tests for validation bypasses caught during the audit.

Each test pins a specific bypass that was reachable before the fix
shipped alongside this file.
"""

from cara.validation.rules import InRule, RequiredRule

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


# ── RequiredRule: collections the original blocklist missed ─────────


def test_required_rejects_empty_frozenset():
    """``frozenset`` is NOT a subclass of ``set`` — they are siblings.
    The original ``(list, tuple, set, dict)`` check therefore let an
    empty frozenset through. Validation must treat all zero-length
    immutable containers as absent for the same reasons it treats
    ``[]`` and ``{}`` as absent."""
    assert RequiredRule().validate("items", frozenset(), {}) is False


def test_required_accepts_non_empty_frozenset():
    assert RequiredRule().validate("items", frozenset({1}), {}) is True


def test_required_rejects_empty_bytes():
    """``b""`` looked "present" to the validator pre-fix because the
    only string check was ``isinstance(value, str)`` — bytes are not
    strings in Python 3. An empty uploaded binary blob should not
    satisfy a ``required`` constraint."""
    assert RequiredRule().validate("blob", b"", {}) is False


def test_required_accepts_non_empty_bytes():
    assert RequiredRule().validate("blob", b"x", {}) is True


def test_required_rejects_empty_bytearray():
    assert RequiredRule().validate("blob", bytearray(), {}) is False


def test_required_accepts_non_empty_bytearray():
    assert RequiredRule().validate("blob", bytearray(b"x"), {}) is True


# ── InRule: non-scalars the original blocklist missed ───────────────


def test_in_rule_rejects_frozenset_input():
    """``frozenset`` is a sibling of ``set``, not a subclass. Pre-fix
    the type guard missed it and the rule then silently failed via
    ``str(frozenset({'a'})) == "frozenset({'a'})"`` not matching the
    allowlist — confusing the caller about WHY the value was rejected."""
    params = {"in": "a,b"}
    assert InRule().validate("fruit", frozenset({"a"}), {**params}) is False


def test_in_rule_rejects_bytes_input():
    """``str(b'a')`` is ``"b'a'"`` — would never match a clean
    allowlist. Reject the type up front instead of producing a
    misleading false-negative."""
    params = {"in": "a,b"}
    assert InRule().validate("fruit", b"a", {**params}) is False


def test_in_rule_rejects_bytearray_input():
    params = {"in": "a,b"}
    assert InRule().validate("fruit", bytearray(b"a"), {**params}) is False


def test_in_rule_accepts_bool():
    """``bool`` is a subclass of ``int`` but a true scalar from form
    input perspective — must pass through the type guard."""
    params = {"in": "True,False"}
    assert InRule().validate("flag", True, {**params}) is True


def test_in_rule_accepts_float():
    params = {"in": "0.5,1.5"}
    assert InRule().validate("ratio", 0.5, {**params}) is True


def test_in_rule_handles_empty_in_param_safely():
    """Missing ``in`` param returns False (no allowlist = nothing
    matches) without raising — defensive against misconfigured rules."""
    assert InRule().validate("x", "a", {}) is False
    assert InRule().validate("x", "a", {"in": ""}) is False


# ── nullable: blank stores None, never the raw blank ────────────────


def test_nullable_blank_string_validates_to_none():
    """Previously: a whitespace-only value on a ``nullable|integer`` field
    skipped the integer rule (correct) but landed in ``validated()`` as the
    RAW ``" "`` (incorrect). ``parse_qs`` keeps blank-only query params, so
    ``?offset=%20`` reached every downstream ``int(v.get("offset") or 0)``
    as ``int(" ")`` → ValueError → an unauthenticated 500 on endpoints whose
    contract is "validation errors are 422". Blank now means null for
    consumers too. (Ported from the cheapa cara copy.)"""
    from cara.validation import Validation

    v = Validation.make(
        {"offset": " ", "min_pct": "\t", "q": ""},
        {
            "offset": "nullable|integer|min:0",
            "min_pct": "nullable|numeric|min:0",
            "q": "nullable|string|max:200",
        },
    )
    assert v.passes()
    validated = v.validated()
    assert validated["offset"] is None
    assert validated["min_pct"] is None
    assert validated["q"] is None


def test_nullable_absent_key_is_omitted_from_validated_payload():
    from cara.validation import Validation

    v = Validation.make({}, {"limit": "nullable|integer|min:1"})
    assert v.passes()
    assert "limit" not in v.validated()


def test_nullable_real_value_still_flows_through():
    from cara.validation import Validation

    v = Validation.make({"offset": "48"}, {"offset": "nullable|integer|min:0"})
    assert v.passes()
    assert v.validated()["offset"] == "48"


def test_sometimes_skips_absent_field_but_validates_explicit_blank():
    from cara.validation import Validation

    omitted = Validation.make(
        {},
        {"cursor": "bail|sometimes|required|string|max:4096"},
    )
    blank = Validation.make(
        {"cursor": ""},
        {"cursor": "bail|sometimes|required|string|max:4096"},
    )

    assert omitted.passes()
    assert "cursor" not in omitted.validated()
    assert blank.fails()
