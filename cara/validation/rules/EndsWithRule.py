"""Ends-with rule. Usage: ``ends_with:.png,.jpg``."""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class EndsWithRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        raw = params.get("ends_with")
        if not raw or value is None:
            return False
        suffixes = [p.strip() for p in raw.split(",")]
        return isinstance(value, str) and any(value.endswith(p) for p in suffixes)

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        raw = params.get("ends_with", "")
        return f"The {attr.lower()} must end with one of: {raw}."
