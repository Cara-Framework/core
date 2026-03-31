"""
Boolean Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is a boolean.
"""

from typing import Any, Dict

from cara.validation.rules import BaseRule


class BooleanRule(BaseRule):
    """Validates that a value is a boolean or boolean-like string."""

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        if value is None:
            return False

        if isinstance(value, bool):
            return True

        if isinstance(value, str):
            return value.lower() in ["true", "false", "1", "0", "yes", "no"]

        if isinstance(value, int):
            return value in [0, 1]

        return False

    def message(self, field: str, params: Dict[str, Any]) -> str:
        return f"'{field}' must be a boolean value."
