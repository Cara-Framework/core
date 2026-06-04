"""Not-in rule. Usage: ``not_in:apple,banana``."""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class NotInRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        raw = params.get("not_in") or params.get("notin")
        if raw is None:
            return True
        forbidden = [v.strip() for v in raw.split(",")]
        return str(value) not in forbidden

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The selected {attr.lower()} is invalid."
