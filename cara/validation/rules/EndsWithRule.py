"""Ends-with rule. Usage: ``ends_with:.png,.jpg``."""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class EndsWithRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        raw = params.get("ends_with") or params.get("endswith")
        if not raw or value is None:
            return False
        suffixes = [p.strip() for p in raw.split(",")]
        return isinstance(value, str) and any(value.endswith(p) for p in suffixes)

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        raw = params.get("ends_with") or params.get("endswith", "")
        return f"The {attr.lower()} must end with one of: {raw}."
