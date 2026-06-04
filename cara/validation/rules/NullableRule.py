"""
Nullable Field Validation Rule for the Cara framework.

This module provides a validation rule that allows null/None values to pass validation.
When a field is nullable, other validation rules are skipped if the value is null.
"""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class NullableRule(BaseRule):
    """Allows null/None values to pass validation."""

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        """Always return True - nullable fields always pass."""
        return True

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        """Return default message (should never be called)."""
        attribute = MessageFormatter.format_attribute_name(field)
        return f"The {attribute.lower()} field validation failed."
