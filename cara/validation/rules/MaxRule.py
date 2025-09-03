"""
Maximum Value Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value doesn't exceed a maximum threshold.
"""

from typing import Any, Dict

from cara.validation.rules import BaseRule


class MaxRule(BaseRule):
    """
    Validates that a string, list, tuple has at most a given length,
    or that a numeric value is at most a given maximum.

    Usage: "max:999"
    """

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        if value is None:
            return False

        # Max parameter is required for this rule
        if "max" not in params:
            return False

        try:
            max_threshold = float(params.get("max"))
        except (TypeError, ValueError):
            return False

        # For numeric values (int, float), compare numerically
        if isinstance(value, (int, float)):
            return float(value) <= max_threshold

        # For strings, lists, tuples, compare length
        elif isinstance(value, (str, list, tuple)):
            return len(value) <= max_threshold

        # Try to convert to float for numeric comparison
        try:
            numeric_value = float(value)
            return numeric_value <= max_threshold
        except (TypeError, ValueError):
            return False

    def message(self, field: str, params: Dict[str, Any]) -> str:
        max_val = params.get("max", "")
        return f"'{field}' may not be greater than {max_val}."
