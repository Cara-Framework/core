"""
Slug Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is a valid slug format.
"""

import re
from typing import Any, Dict

from cara.validation.rules import BaseRule


class SlugRule(BaseRule):
    """Validates that a value is a valid slug (letters, numbers, hyphens, underscores)."""

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        if value is None:
            return False

        if not isinstance(value, str):
            value = str(value)

        # Check if value is a valid slug format: letters, numbers, hyphens, underscores
        return bool(re.match(r"^[\w-]+$", value))

    def message(self, field: str, params: Dict[str, Any]) -> str:
        return f"'{field}' must be a valid slug (letters, numbers, hyphens, and underscores only)."
