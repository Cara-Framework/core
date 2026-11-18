"""
Maximum Value Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value doesn't exceed a maximum threshold.
"""

from __future__ import annotations

from typing import Any

from cara.validation.MessageFormatter import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class MaxRule(BaseRule):
    """
    Validates that a string, list, tuple has at most a given length,
    or that a numeric value is at most a given maximum.

    Usage: "max:999"
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None:
            return False

        # Max parameter is required for this rule
        if "max" not in params:
            return False

        max_threshold = self.as_number(params.get("max"))
        if max_threshold is None:
            return False

        if isinstance(value, bool):
            return False

        chain = params.get("_rules") or ()
        numeric_context = "integer" in chain or "numeric" in chain

        # A negative ``max`` in a LENGTH context can never be satisfied
        # — string / list / dict lengths are always ``>= 0``, so every
        # input fails. Surface as a warning rather than a silent reject
        # so operators have a signal that the rule is misconfigured.
        # Numeric contexts intentionally allow negative thresholds
        # (e.g. ``max:-1`` against a negative-only domain) so we
        # only warn outside that branch.
        if max_threshold < 0 and not numeric_context:
            try:
                from cara.facades import Log  # local: cycle with cara.facades

                Log.warning(
                    "MaxRule misconfig: field=%s has a negative max threshold (%s) in a length context — every input fails by design. Check the rule spec.",
                    field,
                    max_threshold,
                )
            except ImportError, RuntimeError:
                pass

        # Numbers compare as they are — exact, and no float() overflow.
        if isinstance(value, (int, float)):
            return value <= max_threshold

        # Strings: in numeric context compare as number; otherwise by length.
        if isinstance(value, str):
            if numeric_context:
                number = self.as_number(value)
                return number is not None and number <= max_threshold
            return len(value) <= max_threshold

        if isinstance(value, (list, tuple, dict)):
            return len(value) <= max_threshold

        number = self.as_number(value)
        return number is not None and number <= max_threshold

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        max_val = params.get("max", "")
        attribute = MessageFormatter.format_attribute_name(field).lower()
        chain = params.get("_rules") or ()
        length = "integer" not in chain and "numeric" not in chain
        if length and isinstance(params.get("_value"), str):
            return f"The {attribute} may not be greater than {max_val} characters."
        return f"The {attribute} may not be greater than {max_val}."
