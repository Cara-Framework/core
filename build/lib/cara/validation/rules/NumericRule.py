"""
Numeric Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is numeric.
"""

from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class NumericRule(BaseRule):
    """Validates that a value is numeric (int, float, or numeric string)."""

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        """Check if value is numeric."""
        if value is None:
            return False

        if isinstance(value, bool):
            return False

        if isinstance(value, (int, float)):
            return True

        if isinstance(value, str):
            try:
                float(value)
                return True
            except ValueError:
                return False

        return False

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        """Return default numeric validation message."""
        attribute = MessageFormatter.format_attribute_name(field)
        return f"The {attribute.lower()} field must be numeric."
