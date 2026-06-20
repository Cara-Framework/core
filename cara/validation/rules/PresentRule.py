"""Present rule (key must exist; value can be empty). Usage: ``present``."""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class PresentRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        data = params.get("_data", {})
        # Walk dot-separated paths so nested keys like ``user.email``
        # are resolved correctly instead of only checking top-level.
        parts = field.split(".")
        current = data
        for part in parts:
            if not isinstance(current, dict) or part not in current:
                return False
            current = current[part]
        return True

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} field must be present."
