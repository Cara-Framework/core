"""
Max Length Validation Rule for the Cara framework.

This module provides a validation rule that checks if a string value does not exceed a maximum length.
"""

from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class MaxLengthRule(BaseRule):
    """Validates that a string does not exceed a given number of characters.

    Usage: "max_length:255"
    """

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        """Check if string value does not exceed maximum length."""
        if value is None or not isinstance(value, str):
            return False

        max_length = params.get("max_length")
        if max_length is None:
            return False

        threshold = int(max_length)
        return len(value) <= threshold

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        """Return default maximum length validation message."""
        attribute = MessageFormatter.format_attribute_name(field)
        max_val = params.get("max_length", 0)
        return f"The {attribute.lower()} field must not exceed {max_val} characters."
