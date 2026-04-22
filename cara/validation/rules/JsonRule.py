"""JSON-string rule. Usage: ``json``."""
import json
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class JsonRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        if not isinstance(value, str):
            return False
        try:
            json.loads(value)
            return True
        except (ValueError, TypeError):
            return False

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be a valid JSON string."
