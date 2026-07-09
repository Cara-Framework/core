"""Prohibited validation rule. Usage: ``prohibited``."""

from __future__ import annotations

from typing import Any

from cara.validation.rules.BaseRule import BaseRule


class ProhibitedRule(BaseRule):
    """Validates that a field is NOT present or is empty.

    The inverse of ``required`` — the field must be absent or have a null/empty value.
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        data = params.get("_data", {})
        if not self.field_present(data, field):
            return True
        if value is None:
            return True
        return bool(isinstance(value, str) and value.strip() == "")

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        return f"'{field}' is prohibited."
