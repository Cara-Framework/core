"""
Alphanumeric Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value contains only alphanumeric characters.
"""

import re
from typing import Any, Dict

from cara.validation.rules import BaseRule


class AlphanumRule(BaseRule):
    """Validates that a value contains only alphanumeric characters."""

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        if value is None:
            return False

        if not isinstance(value, str):
            value = str(value)

        # Check if value contains only alphanumeric characters
        return bool(re.match(r"^[a-zA-Z0-9]+$", value))

    def message(self, field: str, params: Dict[str, Any]) -> str:
        return f"'{field}' must contain only alphanumeric characters."
