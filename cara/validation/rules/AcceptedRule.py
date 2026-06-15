"""Accepted rule (truthy for checkbox-like fields). Usage: ``accepted``."""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule

_TRUTHY = {"yes", "on", "1", 1, "true"}


class AcceptedRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if isinstance(value, str):
            return value.lower() in {"yes", "on", "1", "true"}
        return value in _TRUTHY

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be accepted."
