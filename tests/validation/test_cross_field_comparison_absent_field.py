"""``lt`` / ``gt`` / ``lte`` / ``gte`` referencing another field.

All four rules accept either a literal numeric threshold (``lte:10``)
or a field-name reference (``lte:max_price``). The field-reference form
is the declarative way to say "A must not exceed B" — and a range
filter is normally single-bound: the caller sends only ``min_price``,
or only ``max_price``.

The rule therefore has to be a NO-OP when the referenced field is
absent. There is no other side to compare against, so there is nothing
to violate; failing instead turns every single-bound request into a
422 with a message naming a field the caller never sent. The guard is
one line per rule (``if other is None and _to_number(threshold) is
None: return True``) and it is exactly the line a "let's collapse
these four rules into a shared helper" refactor drops.

All four are pinned together because they were fixed at different
times: the ``lte`` / ``gte`` pair first, the strict ``lt`` / ``gt``
siblings only afterwards — the asymmetry is the failure mode.
"""

from __future__ import annotations

import pytest

from cara.validation.rules import GteRule, GtRule, LteRule, LtRule

# (rule class, the params key it reads, whether the comparison is strict)
_LOWER_BOUND_RULES = [
    (LtRule, "lt", True),
    (LteRule, "lte", False),
]
_UPPER_BOUND_RULES = [
    (GtRule, "gt", True),
    (GteRule, "gte", False),
]
_ALL_RULES = _LOWER_BOUND_RULES + _UPPER_BOUND_RULES


def _check(rule_cls, key: str, field: str, value, data: dict, threshold: str) -> bool:
    """Invoke a rule the way ``Validator`` builds its params at runtime."""
    return rule_cls().validate(field, value, {key: threshold, "_data": data})


# ── The headline contract: absent referenced field is a no-op ─────────


class TestAbsentReferencedFieldPasses:
    """A single-bound payload must validate."""

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _ALL_RULES)
    def test_absent_field_passes(self, rule_cls, key: str, _strict: bool) -> None:
        """``min_price=50`` with no ``max_price`` in the payload."""
        assert (
            _check(rule_cls, key, "min_price", 50, {"min_price": 50}, "max_price")
            is True
        )

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _ALL_RULES)
    def test_referenced_field_present_but_null_also_passes(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """``{"max_price": None}`` is the same "no bound" statement.

        The lookup cannot distinguish an absent key from a null value,
        and treating an explicit null as a violation would reject the
        JSON shape most clients send for "unset".
        """
        assert (
            _check(
                rule_cls,
                key,
                "min_price",
                50,
                {"min_price": 50, "max_price": None},
                "max_price",
            )
            is True
        )


# ── The comparison still happens when both sides are there ───────────


class TestPresentReferencedFieldIsCompared:
    """The no-op guard must not swallow real violations."""

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _LOWER_BOUND_RULES)
    def test_lower_bound_within_range_passes(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """50 is below 100 under both ``lt`` and ``lte``."""
        data = {"min_price": 50, "max_price": 100}
        assert _check(rule_cls, key, "min_price", 50, data, "max_price") is True

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _LOWER_BOUND_RULES)
    def test_inverted_bounds_are_rejected(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """500 below 100 is false under both."""
        data = {"min_price": 500, "max_price": 100}
        assert _check(rule_cls, key, "min_price", 500, data, "max_price") is False

    @pytest.mark.parametrize(("rule_cls", "key", "strict"), _LOWER_BOUND_RULES)
    def test_equal_values_separate_strict_from_inclusive(
        self, rule_cls, key: str, strict: bool
    ) -> None:
        """``lt`` rejects equality, ``lte`` accepts it — the whole
        difference between the two rules."""
        data = {"min_price": 100, "max_price": 100}
        result = _check(rule_cls, key, "min_price", 100, data, "max_price")
        assert result is (not strict)

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _UPPER_BOUND_RULES)
    def test_upper_bound_within_range_passes(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """100 is above 50 under both ``gt`` and ``gte``."""
        data = {"min_price": 50, "max_price": 100}
        assert _check(rule_cls, key, "max_price", 100, data, "min_price") is True

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _UPPER_BOUND_RULES)
    def test_inverted_bounds_are_rejected_upper(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """10 above 500 is false under both."""
        data = {"min_price": 500, "max_price": 10}
        assert _check(rule_cls, key, "max_price", 10, data, "min_price") is False

    @pytest.mark.parametrize(("rule_cls", "key", "strict"), _UPPER_BOUND_RULES)
    def test_equal_values_separate_strict_from_inclusive_upper(
        self, rule_cls, key: str, strict: bool
    ) -> None:
        """Mirror of the lower-bound equality case."""
        data = {"min_price": 50, "max_price": 50}
        result = _check(rule_cls, key, "max_price", 50, data, "min_price")
        assert result is (not strict)

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _ALL_RULES)
    def test_numeric_strings_on_either_side_are_compared(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """Query-string values arrive as strings; both sides coerce."""
        data = {"a": "50", "b": "100"}
        lower = _check(rule_cls, key, "a", "50", data, "b")
        upper = _check(rule_cls, key, "b", "100", data, "a")
        # Exactly one direction holds, whichever way the rule points.
        assert lower is not upper


# ── Literal numeric thresholds are untouched by the guard ────────────


class TestLiteralThresholds:
    """The guard is for the field-reference form only."""

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _LOWER_BOUND_RULES)
    def test_literal_upper_bound_is_enforced(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """``lt:100`` / ``lte:100`` still reject 150."""
        assert _check(rule_cls, key, "qty", 50, {"qty": 50}, "100") is True
        assert _check(rule_cls, key, "qty", 150, {"qty": 150}, "100") is False

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _UPPER_BOUND_RULES)
    def test_literal_lower_bound_is_enforced(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """``gt:10`` / ``gte:10`` still reject 5."""
        assert _check(rule_cls, key, "qty", 50, {"qty": 50}, "10") is True
        assert _check(rule_cls, key, "qty", 5, {"qty": 5}, "10") is False

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _LOWER_BOUND_RULES)
    def test_a_zero_literal_is_not_mistaken_for_absent(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """The guard tests ``_to_number(threshold) is None``, not its
        truthiness. A truthiness test would make ``lt:0`` a no-op and
        let every positive value through a "must be negative" rule."""
        assert _check(rule_cls, key, "delta", -5, {"delta": -5}, "0") is True
        assert _check(rule_cls, key, "delta", 5, {"delta": 5}, "0") is False


# ── Non-comparable inputs still fail ─────────────────────────────────


class TestNonComparableInputs:
    """The no-op guard must not become a blanket pass."""

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _ALL_RULES)
    def test_a_missing_rule_argument_fails(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """No threshold at all is a broken rule, not an absent field."""
        assert rule_cls().validate("qty", 50, {"_data": {"qty": 50}}) is False

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _ALL_RULES)
    def test_a_null_value_fails(self, rule_cls, key: str, _strict: bool) -> None:
        """Nullability is ``nullable``'s job, not this rule's."""
        assert _check(rule_cls, key, "qty", None, {"qty": None}, "100") is False

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _ALL_RULES)
    def test_a_non_numeric_value_fails(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """``"abc"`` cannot be ordered against a number."""
        assert _check(rule_cls, key, "qty", "abc", {"qty": "abc"}, "100") is False

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _ALL_RULES)
    def test_a_present_but_non_numeric_referenced_field_fails(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """The field IS there, so the guard does not fire — and a
        non-numeric bound cannot be compared, so the rule fails rather
        than passing by accident."""
        data = {"a": 50, "b": "not-a-number"}
        assert _check(rule_cls, key, "a", 50, data, "b") is False


class TestMalformedThresholdIsIndistinguishableFromAnAbsentField:
    """Documented consequence of the guard, pinned deliberately.

    A threshold that is neither a number nor a field present in the
    payload — a typo like ``lte:maxprice`` — takes the same branch as a
    legitimately absent reference and PASSES. The rule string alone
    carries no way to tell the two apart at validation time, so the
    guard cannot separate them without a schema of declared field
    names. Pinned so the behaviour is a known trade-off rather than a
    surprise: a typo in a comparison rule disables that rule silently.
    """

    @pytest.mark.parametrize(("rule_cls", "key", "_strict"), _ALL_RULES)
    def test_a_typoed_field_reference_passes(
        self, rule_cls, key: str, _strict: bool
    ) -> None:
        """No exception, no failure — the rule simply stops applying."""
        data = {"min_price": 50, "max_price": 10}
        assert _check(rule_cls, key, "min_price", 50, data, "maxprice") is True
