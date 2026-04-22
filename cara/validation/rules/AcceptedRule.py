"""Accepted rule (truthy for checkbox-like fields). Usage: ``accepted``."""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


_TRUTHY = {"yes", "on", "1", 1, True, "true"}


class AcceptedRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        if isinstance(value, str):
            return value.lower() in {"yes", "on", "1", "true"}
        return value in _TRUTHY

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be accepted."
