"""JSON-string rule. Usage: ``json``."""

from __future__ import annotations

import json
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class JsonRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if not isinstance(value, str):
            return False
        try:
            json.loads(value)
            return True
        except ValueError, TypeError:
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be a valid JSON string."
