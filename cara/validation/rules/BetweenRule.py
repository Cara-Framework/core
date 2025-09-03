"""
Between Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is between two given values.
"""

from typing import Any, Dict

from cara.validation.rules import BaseRule


class BetweenRule(BaseRule):
    """
    Validates that a numeric value or string length is between two values.

    Usage: "between:5,10"
    """

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        if value is None:
            return False

        between_param = params.get("between")
        if not between_param:
            return False

        try:
            min_val, max_val = between_param.split(",", 1)
            min_val = float(min_val.strip())
            max_val = float(max_val.strip())
        except (ValueError, AttributeError):
            return False

        if isinstance(value, (str, list, tuple)):
            length = len(value)
            return min_val <= length <= max_val

        if isinstance(value, (int, float)):
            return min_val <= value <= max_val

        return False

    def message(self, field: str, params: Dict[str, Any]) -> str:
        between_param = params.get("between", "")
        return f"'{field}' must be between {between_param}."
