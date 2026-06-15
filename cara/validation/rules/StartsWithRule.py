"""Starts-with rule. Usage: ``starts_with:http,https``."""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class StartsWithRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        raw = params.get("starts_with") or params.get("startswith")
        if not raw or value is None:
            return False
        prefixes = [p.strip() for p in raw.split(",")]
        return isinstance(value, str) and any(value.startswith(p) for p in prefixes)

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        raw = params.get("starts_with") or params.get("startswith", "")
        return f"The {attr.lower()} must start with one of: {raw}."
