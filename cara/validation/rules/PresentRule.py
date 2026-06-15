"""Present rule (key must exist; value can be empty). Usage: ``present``."""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class PresentRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        data = params.get("_data", {})
        # Value may be None or '' but the KEY must be present in payload.
        return field in data

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} field must be present."
