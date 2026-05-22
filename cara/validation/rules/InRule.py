"""
In Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is in a given list of values.
"""

from typing import Any

from cara.validation.rules import BaseRule


class InRule(BaseRule):
    """
    Validates that a value is in a given list of acceptable values.

    Usage: "in:apple,banana,orange"
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None:
            return False

        # Reject non-scalar inputs explicitly. Without this guard, a
        # caller could submit ``["apple"]`` and the rule would compare
        # ``str(["apple"])`` (i.e. ``"['apple']"``) against the
        # allowlist — a confusing silent fail. Worse, ``str(value)``
        # against an attacker-controlled object can hit ``__str__`` /
        # ``__repr__`` side effects.
        if isinstance(value, (list, tuple, set, dict)):
            return False

        in_values = params.get("in")
        if not in_values:
            return False

        values_list = [v.strip() for v in in_values.split(",")]
        return str(value) in values_list

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        in_values = params.get("in", "")
        return f"'{field}' must be one of: {in_values}."
