"""
Integer Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is an integer.
"""

from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class IntegerRule(BaseRule):
    """Validates that a value is an integer or integer string."""

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        """Check if value is an integer."""
        if value is None:
            return False

        if isinstance(value, bool):
            return False

        if isinstance(value, int):
            return True

        if isinstance(value, str):
            try:
                int(value)
                return True
            except ValueError:
                return False

        return False

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        """Return default integer validation message."""
        attribute = MessageFormatter.format_attribute_name(field)
        return f"The {attribute.lower()} field must be an integer."
