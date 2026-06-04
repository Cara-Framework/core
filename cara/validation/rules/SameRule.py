"""Same rule. Usage: ``same:other_field``."""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class SameRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        other = params.get("same")
        if not other:
            return False
        data = params.get("_data", {})
        return value == data.get(other)

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} and {params.get('same', '')} must match."
