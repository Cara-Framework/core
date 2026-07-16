"""Missing validation rule. Usage: ``missing``."""

from __future__ import annotations

from typing import Any

from cara.validation.rules.BaseRule import BaseRule


class MissingRule(BaseRule):
    """Validate that the field is not present in the input at all."""

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        return not self.field_present(params.get("_data", {}), field)

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        return f"'{field}' must not be present."
