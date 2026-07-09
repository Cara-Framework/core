"""Filled validation rule. Usage: ``filled``."""

from __future__ import annotations

from typing import Any

from cara.validation.rules.BaseRule import BaseRule


class FilledRule(BaseRule):
    """Validates that a field, when present, is not empty.

    Unlike ``required``, ``filled`` does not mandate the field be present.
    However, if the field exists in the input, it must have a non-empty value.
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        data = params.get("_data", {})
        if not self.field_present(data, field):
            return True
        if value is None:
            return False
        if isinstance(value, str) and value.strip() == "":
            return False
        return not (isinstance(value, (list, dict)) and len(value) == 0)

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        return f"'{field}' must not be empty when present."
