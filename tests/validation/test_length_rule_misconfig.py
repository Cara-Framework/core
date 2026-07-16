"""``min_length`` / ``max_length`` must NOT crash on a misconfigured
non-numeric threshold.

Pre-fix the rule body did a bare ``threshold = int(min_length)`` —
a typo'd rule literal like ``min_length:abc`` (or a config-supplied
threshold that got the wrong template-interpolated value) raised
``ValueError`` straight out of the validator. The framework's
exception handler turned that into a 500 response for any request
that touched the misconfigured field.

The sibling numeric rules (``MinRule`` / ``MaxRule``) already had
the misconfig-warning path landed earlier (cara commit e437641).
This brings the length rules in line with that contract:

  * Non-numeric threshold → log a ``cara.validation`` warning so
    ops can grep for the typo, AND fail the value (defensive
    default — for a length CAP, silently letting every value
    through would mask the typo until something happens to be
    too long by accident; failing makes the misconfiguration
    visible on the first request).
  * Numeric threshold path unchanged — fast happy path stays.

The defensive-fail-on-misconfig choice differs from the numeric
``MinRule``/``MaxRule`` (which log-and-pass) because the typical
length cap exists to bound DB column width or API payload size —
silent pass-through could let a 100KB blob land in a varchar(255)
column. Numeric rules guard semantic bounds where a pass-through
is at worst a missed assertion, not a column overflow.
"""

from __future__ import annotations

import pytest

from cara.validation.rules import MaxLengthRule, MinLengthRule


class TestMinLengthMisconfig:
    def setup_method(self):
        self.rule = MinLengthRule()

    def test_non_numeric_threshold_does_not_raise(self):
        # The bug shape: pre-fix this raised ValueError mid-validation
        # and turned into a 500 response. MUST return False instead.
        params = {"min_length": "abc"}
        assert self.rule.validate("name", "short", params) is False

    def test_empty_string_threshold_does_not_raise(self):
        params = {"min_length": ""}
        assert self.rule.validate("name", "anything", params) is False

    def test_list_threshold_does_not_raise(self):
        # int([5]) → TypeError, not ValueError — the except catches both.
        params = {"min_length": [5]}
        assert self.rule.validate("name", "ok", params) is False

    def test_numeric_string_threshold_still_works(self):
        # Sanity: the happy path is preserved.
        params = {"min_length": "5"}
        assert self.rule.validate("name", "longer-than-five", params) is True
        assert self.rule.validate("name", "shrt", params) is False

    def test_integer_threshold_still_works(self):
        params = {"min_length": 5}
        assert self.rule.validate("name", "longer-than-five", params) is True
        assert self.rule.validate("name", "shrt", params) is False


class TestMaxLengthMisconfig:
    def setup_method(self):
        self.rule = MaxLengthRule()

    def test_non_numeric_threshold_does_not_raise(self):
        params = {"max_length": "abc"}
        assert self.rule.validate("name", "anything", params) is False

    def test_empty_string_threshold_does_not_raise(self):
        params = {"max_length": ""}
        assert self.rule.validate("name", "anything", params) is False

    def test_list_threshold_does_not_raise(self):
        params = {"max_length": [255]}
        assert self.rule.validate("name", "ok", params) is False

    def test_numeric_string_threshold_still_works(self):
        params = {"max_length": "10"}
        assert self.rule.validate("name", "short", params) is True
        assert self.rule.validate("name", "x" * 11, params) is False

    def test_integer_threshold_still_works(self):
        params = {"max_length": 10}
        assert self.rule.validate("name", "short", params) is True
        assert self.rule.validate("name", "x" * 11, params) is False


class TestBoundaryStillTight:
    """Edge values around the cap — pin that the rule didn't soften
    while we made it crash-safe."""

    def test_min_length_exact_threshold_passes(self):
        rule = MinLengthRule()
        assert rule.validate("x", "x" * 5, {"min_length": 5}) is True

    def test_max_length_exact_threshold_passes(self):
        rule = MaxLengthRule()
        assert rule.validate("x", "x" * 5, {"max_length": 5}) is True

    @pytest.mark.parametrize("bad_value", [None, 42, 3.14, [], {}])
    def test_non_string_value_fails(self, bad_value):
        # Both rules reject non-string values up-front (line 21 in
        # each: ``if value is None or not isinstance(value, str)``).
        # Pin so a future refactor that broadens to len(value) on
        # arrays / dicts is a deliberate choice, not a slip.
        assert MinLengthRule().validate("x", bad_value, {"min_length": 1}) is False
        assert MaxLengthRule().validate("x", bad_value, {"max_length": 100}) is False
