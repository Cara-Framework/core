"""
Minimum Value Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value meets a minimum threshold.
"""

from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class MinRule(BaseRule):
    """
    Validates that a string, list, tuple has at least a given length,
    or that a numeric value is at least a given minimum.

    Usage: "min:5"
    """

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        """Check if value meets minimum threshold."""
        if value is None:
            return False

        # Min parameter is required for this rule
        if "min" not in params:
            return False

        try:
            min_threshold = float(params.get("min"))
        except (TypeError, ValueError):
            return False

        # For numeric values (int, float), compare numerically
        if isinstance(value, (int, float)):
            return float(value) >= min_threshold

        # For strings, lists, tuples, compare length
        elif isinstance(value, (str, list, tuple)):
            return len(value) >= min_threshold

        # Try to convert to float for numeric comparison
        try:
            numeric_value = float(value)
            return numeric_value >= min_threshold
        except (TypeError, ValueError):
            return False

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        """Return default minimum value message."""
        min_val = params.get("min", "")
        attribute = MessageFormatter.format_attribute_name(field)

        # Smart message based on value type
        if isinstance(params.get("_value"), str):
            return f"The {attribute.lower()} field must be at least {min_val} characters."
        else:
            return f"The {attribute.lower()} field must be at least {min_val}."
