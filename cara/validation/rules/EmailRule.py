"""
Email Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is a valid email address.
"""

import re
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class EmailRule(BaseRule):
    """Validates that a string is a well‐formed email address."""

    _pattern = re.compile(r"^[\w\.\-\+]+@[\w\.\-]+\.\w+$")

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        """Check if value is a valid email format."""
        if value is None or not isinstance(value, str):
            return False
        return bool(self._pattern.match(value))

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        """Return default email validation message."""
        attribute = MessageFormatter.format_attribute_name(field)
        return f"The {attribute.lower()} field must be a valid email address."
