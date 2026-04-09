"""
Min Length Validation Rule for the Cara framework.

This module provides a validation rule that checks if a string value meets a minimum length.
"""

from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class MinLengthRule(BaseRule):
    """Validates that a string has at least a given number of characters.

    Usage: "min_length:5"
    """

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        """Check if string value meets minimum length requirement."""
        if value is None or not isinstance(value, str):
            return False

        min_length = params.get("min_length")
        if min_length is None:
            return False

        threshold = int(min_length)
        return len(value) >= threshold

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        """Return default minimum length validation message."""
        attribute = MessageFormatter.format_attribute_name(field)
        min_val = params.get("min_length", 0)
        return f"The {attribute.lower()} field must be at least {min_val} characters long."
