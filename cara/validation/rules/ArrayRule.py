"""
Array validation rule.

Validates that a value is a list/array.
"""

from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class ArrayRule(BaseRule):
    """Validates that a value is a list/array."""

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        """
        Check if the value is a list/array.

        Args:
            field: The field name being validated
            value: The value to validate
            params: Validation parameters

        Returns:
            True if value is a list, False otherwise
        """
        if value is None:
            return False
        return isinstance(value, list)

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        """
        Get the validation error message.

        Args:
            field: The field name being validated
            params: Validation parameters

        Returns:
            The error message
        """
        attribute = MessageFormatter.format_attribute_name(field)
        return f"The {attribute.lower()} must be an array."
