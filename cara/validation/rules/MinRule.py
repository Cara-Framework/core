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

        if isinstance(value, bool):
            return False

        chain = params.get("_rules") or ()
        numeric_context = "integer" in chain or "numeric" in chain

        # For numeric values (int, float), compare numerically
        if isinstance(value, (int, float)):
            return float(value) >= min_threshold

        # Strings: in numeric context compare as number; otherwise by length.
        if isinstance(value, str):
            if numeric_context:
                try:
                    return float(value) >= min_threshold
                except ValueError:
                    return False
            return len(value) >= min_threshold

        # Lists/tuples/dicts compare by length.
        if isinstance(value, (list, tuple, dict)):
            return len(value) >= min_threshold

        # Last-ditch numeric conversion.
        try:
            return float(value) >= min_threshold
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
