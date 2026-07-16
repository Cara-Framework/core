"""Cross-field rule edge cases for ``required_if`` / ``required_unless``.

Both rules parsed the rule literal off a string (``required_if:flag,false``)
and compared the runtime payload value via the bare ``str(actual) == expected``
form. Two real-world failure modes the bare form silently produced:

  1. **Python bools never matched their lowercase rule literal.** The
     canonical Laravel-style rule ``required_if:is_active,false`` is
     lowercase but ``str(False)`` returns ``'False'`` (capital ``F``),
     so the comparison was always ``False`` and the field was never
     treated as required when ``is_active`` came in as a real bool
     (the common case — JSON bodies decode ``"is_active": false``
     directly to a Python bool).

  2. **Case-sensitive against form-uppercased HTTP input.** A field
     coming back as ``role="ADMIN"`` from a form that uppercased its
     ``<select>`` values never matched ``required_if:role,Admin``,
     even though the rule was written specifically to target that
     value.

The fix routes both rules through ``_values_match`` (lives in
RequiredIfRule, imported by RequiredUnlessRule so the comparison stays
in lockstep). The helper:

  * Branches on ``isinstance(actual, bool)`` BEFORE str-coerce so the
    capitalised Python repr never reaches the comparison — bools are
    lower-cased on both sides directly.
  * Falls through to ``str(actual).strip().lower() ==
    expected.strip().lower()`` for everything else, so numeric
    coincidence (``"5" == 5`` via ``str(5)``) and case-insensitive
    string match both work.
  * Recognises ``None`` against the literals ``"none"`` / ``"null"``
    so a ``required_if:status,null`` rule can target an explicitly-
    null upstream value.

Error messages were also stale — ``"…when {other} equals the given
value."`` named the gating field but hid the actual expected value
the user needed to satisfy. Both rules now surface ``'{expected}'``
in the default message when the rule literal supplied one.
"""

from __future__ import annotations

import pytest

from cara.validation.rules import RequiredIfRule, RequiredUnlessRule
from cara.validation.rules.RequiredIfRule import _values_match

# ── _values_match — the underlying comparison ───────────────────────


class TestValuesMatch:
    @pytest.mark.parametrize(
        "actual,expected,want",
        [
            # Python bool against the canonical lowercase rule literal.
            (True, "true", True),
            (False, "false", True),
            (True, "false", False),
            (False, "true", False),
            # Bool against uppercase rule literal — both sides lower-cased,
            # so the rule author can write whichever they prefer.
            (True, "TRUE", True),
            (False, "False", True),
            # String case-insensitive (form-uppercased select values).
            ("ADMIN", "admin", True),
            ("admin", "Admin", True),
            # Whitespace tolerant — handles trailing-newline rule literals
            # that survived a copy-paste from docs.
            ("active", " active ", True),
            # None against the documented sentinel literals.
            (None, "null", True),
            (None, "none", True),
            (None, "active", False),
            # Numeric coincidence still works for int<->str pairs.
            (18, "18", True),
            ("18", "18", True),
            # Negative case — different values.
            ("inactive", "active", False),
        ],
    )
    def test_match_truth_table(self, actual, expected, want):
        assert _values_match(actual, expected) is want

    def test_isinstance_bool_branches_before_int(self):
        # ``isinstance(True, int)`` is True in Python — the bool
        # branch MUST come first or ``True``/``1`` ambiguity would
        # collapse the comparison. Pin the order so a refactor that
        # swaps branches fires here.
        assert _values_match(True, "true") is True
        assert _values_match(1, "true") is False  # int 1 stringifies "1"


# ── RequiredIfRule end-to-end ────────────────────────────────────────


class TestRequiredIfBoolGate:
    def setup_method(self):
        self.rule = RequiredIfRule()

    def test_bool_false_required_if_other_field_is_lowercase_false_literal(self):
        # The regression: pre-fix this rule literal never matched.
        params = {
            "required_if": "is_active,false",
            "_data": {"is_active": False},
        }
        # Field is required (gate matched), value is empty → fail.
        assert self.rule.validate("reason", "", params) is False
        # Field is required (gate matched), value provided → pass.
        assert self.rule.validate("reason", "manual deactivate", params) is True

    def test_bool_true_required_if_other_field_is_lowercase_true_literal(self):
        params = {
            "required_if": "is_published,true",
            "_data": {"is_published": True},
        }
        assert self.rule.validate("slug", None, params) is False
        assert self.rule.validate("slug", "my-post", params) is True

    def test_uppercase_form_value_matches_lowercase_rule_literal(self):
        params = {
            "required_if": "tier,enterprise",
            "_data": {"tier": "ENTERPRISE"},  # HTML form uppercased
        }
        assert self.rule.validate("billing_contact", None, params) is False

    def test_gate_does_not_match_when_other_field_absent(self):
        params = {"required_if": "tier,enterprise", "_data": {}}
        # Other field absent → gate not met → field NOT required.
        assert self.rule.validate("billing_contact", None, params) is True

    def test_gate_does_not_match_when_other_field_is_different_value(self):
        params = {
            "required_if": "tier,enterprise",
            "_data": {"tier": "free"},
        }
        assert self.rule.validate("billing_contact", None, params) is True


class TestRequiredIfMessage:
    def setup_method(self):
        self.rule = RequiredIfRule()

    def test_message_includes_expected_value(self):
        msg = self.rule.default_message(
            "billing_contact",
            {"required_if": "tier,enterprise"},
        )
        assert "tier" in msg
        assert "enterprise" in msg, (
            f"Message must surface the gating value so the user knows "
            f"which payload state triggered the requirement; got: {msg!r}"
        )

    def test_message_falls_back_when_expected_missing(self):
        # Malformed rule literal — single segment, no expected value.
        # Should still render readably, NOT raise IndexError.
        msg = self.rule.default_message("foo", {"required_if": "bar"})
        assert "foo" in msg
        assert "bar" in msg


# ── RequiredUnlessRule mirror coverage ───────────────────────────────


class TestRequiredUnlessBoolGate:
    def setup_method(self):
        self.rule = RequiredUnlessRule()

    def test_bool_false_required_unless_other_field_is_lowercase_false_literal(self):
        # required_unless inverts: condition met → NOT required.
        params = {
            "required_unless": "is_anonymous,false",
            "_data": {"is_anonymous": False},
        }
        # is_anonymous == false → "unless" gate matched → NOT required.
        assert self.rule.validate("display_name", None, params) is True

    def test_bool_true_required_unless_other_field_is_uppercase_true_literal(self):
        # Pre-fix this case was a SILENT INVERSION — Python's
        # ``str(True) == "true"`` returned False (capital T) so the
        # "unless" gate never matched, and the field was wrongly
        # treated as required when it shouldn't have been.
        params = {
            "required_unless": "is_anonymous,true",
            "_data": {"is_anonymous": True},
        }
        assert self.rule.validate("display_name", None, params) is True, (
            "is_anonymous=True (Python bool) MUST match the rule "
            "literal 'true' so the 'unless' gate triggers and "
            "display_name is NOT required. Pre-fix the bool stringified "
            "to 'True' (capital T) and the gate never matched, so "
            "anonymous users were wrongly forced to supply a display_name."
        )

    def test_condition_not_met_requires_field(self):
        params = {
            "required_unless": "is_anonymous,true",
            "_data": {"is_anonymous": False},
        }
        # is_anonymous=False → "unless true" condition NOT met → required.
        assert self.rule.validate("display_name", None, params) is False
        assert self.rule.validate("display_name", "Alice", params) is True


class TestRequiredUnlessMessage:
    def setup_method(self):
        self.rule = RequiredUnlessRule()

    def test_message_includes_other_field_and_expected(self):
        msg = self.rule.default_message(
            "display_name",
            {"required_unless": "is_anonymous,true"},
        )
        assert "is_anonymous" in msg
        assert "true" in msg

    def test_message_falls_back_when_rule_literal_malformed(self):
        msg = self.rule.default_message("foo", {})
        assert "foo" in msg
