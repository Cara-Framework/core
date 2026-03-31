"""
In Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is in a given list of values.
"""

from typing import Any, Dict

from cara.validation.rules import BaseRule


class InRule(BaseRule):
    """
    Validates that a value is in a given list of acceptable values.

    Usage: "in:apple,banana,orange"
    """

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        if value is None:
            return False

        in_values = params.get("in")
        if not in_values:
            return False

        values_list = [v.strip() for v in in_values.split(",")]
        return str(value) in values_list

    def message(self, field: str, params: Dict[str, Any]) -> str:
        in_values = params.get("in", "")
        return f"'{field}' must be one of: {in_values}."
