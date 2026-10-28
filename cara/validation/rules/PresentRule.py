"""Present rule (key must exist; value can be empty). Usage: ``present``."""

from __future__ import annotations

from typing import Any

from cara.validation.MessageFormatter import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class PresentRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        # The shared dotted-path walk: wildcard-expanded fields arrive as
        # ``items.0.name`` and must descend through list indexes too.
        return self.field_present(params.get("_data", {}), field)

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} field must be present."
