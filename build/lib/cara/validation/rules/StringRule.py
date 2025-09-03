"""
String Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is a string.
"""

from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class StringRule(BaseRule):
    """Validates that a value is a string."""

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        """Check if value is a string."""
        if value is None:
            return False
        return isinstance(value, str)

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        """Return default string validation message."""
        attribute = MessageFormatter.format_attribute_name(field)
        return f"The {attribute.lower()} field must be a string."
