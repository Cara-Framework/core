"""``between:1,100`` speaks its bounds as words, never as the rule literal.

An API customer sending ``limit=0`` read "'limit' must be between 1,100." —
which reads as one thousand one hundred (api-consumer round, 2026-11-12). The
message names both bounds: "between 1 and 100".
"""

from __future__ import annotations

from cara.validation.rules.BetweenRule import BetweenRule


def test_the_message_names_both_bounds_in_words() -> None:
    message = BetweenRule().default_message("limit", {"between": "1,100"})
    assert message == "'limit' must be between 1 and 100."


def test_a_spaced_or_decimal_literal_reads_the_same_way() -> None:
    rule = BetweenRule()
    assert rule.default_message("rate", {"between": "0.5, 2"}) == (
        "'rate' must be between 0.5 and 2."
    )


def test_a_malformed_literal_is_said_as_written() -> None:
    assert BetweenRule().default_message("x", {"between": "7"}) == (
        "'x' must be between 7."
    )


def test_digits_between_names_both_bounds_in_words() -> None:
    """The sibling rule printed the same literal: "'code' must be between 3,8
    digits." (api-consumer review, 2026-11-12)."""
    from cara.validation.rules.DigitsBetweenRule import DigitsBetweenRule

    rule = DigitsBetweenRule()
    assert rule.default_message("code", {"digits_between": "3,8"}) == (
        "'code' must have between 3 and 8 digits."
    )
    assert rule.default_message("code", {"digits_between": "7"}) == (
        "'code' must have between 7 digits."
    )
