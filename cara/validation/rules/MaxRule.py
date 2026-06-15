"""
Maximum Value Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value doesn't exceed a maximum threshold.
"""

from __future__ import annotations

from typing import Any

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

        try:
            max_threshold = float(params.get("max"))
        except (TypeError, ValueError):
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
                from cara.facades import Log

                Log.warning("MaxRule misconfig: field=%s has a negative max threshold (%s) in a length context — every input fails by design. Check the rule spec.", field, max_threshold)
            except (ImportError, RuntimeError):
                pass

        # For numeric values (int, float), compare numerically
        if isinstance(value, (int, float)):
            return float(value) <= max_threshold

        # Strings: in numeric context compare as number; otherwise by length.
        if isinstance(value, str):
            if numeric_context:
                try:
                    return float(value) <= max_threshold
                except ValueError:
                    return False
            return len(value) <= max_threshold

        if isinstance(value, (list, tuple, dict)):
            return len(value) <= max_threshold

        try:
            return float(value) <= max_threshold
        except (TypeError, ValueError):
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        max_val = params.get("max", "")
        return f"'{field}' may not be greater than {max_val}."
