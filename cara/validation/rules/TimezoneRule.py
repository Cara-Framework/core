"""Timezone rule (IANA name, e.g. ``Europe/Istanbul``). Usage: ``timezone``."""

from __future__ import annotations

from typing import Any
from zoneinfo import ZoneInfo

from cara.validation.MessageFormatter import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class TimezoneRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if not isinstance(value, str):
            return False
        try:
            ZoneInfo(value)
            return True
        except KeyError, ValueError, TypeError:
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be a valid timezone."
