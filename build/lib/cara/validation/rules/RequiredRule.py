"""
Required Field Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is present and not empty.
"""

from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class RequiredRule(BaseRule):
    """Validates that a value is not None and not an empty string."""

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        """Check if value is present and not empty."""
        if value is None:
            return False
        if isinstance(value, str) and value.strip() == "":
            return False
        return True

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        """Return default required field message."""
        attribute = MessageFormatter.format_attribute_name(field)
        return f"The {attribute.lower()} field is required."
