"""
Confirmed Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value matches its confirmation field.
"""

from __future__ import annotations

from typing import Any

from cara.validation.rules import BaseRule


class ConfirmedRule(BaseRule):
    """
    Validates that a field has a matching confirmation field.

    Usage: "confirmed" (looks for field_confirmation)
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None:
            return False

        confirmation_field = f"{field}_confirmation"
        confirmation_value = params.get("_data", {}).get(confirmation_field)

        return value == confirmation_value

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        return f"'{field}' confirmation does not match."
