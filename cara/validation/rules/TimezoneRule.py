"""Timezone rule (IANA name, e.g. ``Europe/Istanbul``). Usage: ``timezone``."""

from __future__ import annotations

from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule

try:
    from zoneinfo import ZoneInfo, available_timezones
except ImportError:  # py < 3.9 fallback (shouldn't hit in this codebase)
    ZoneInfo = None  # type: ignore

    def available_timezones() -> set:  # type: ignore[no-redef]
        """Fallback when ``zoneinfo`` isn't on the import path."""
        return set()


class TimezoneRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if not isinstance(value, str) or ZoneInfo is None:
            return False
        try:
            ZoneInfo(value)
            return True
        except Exception:
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be a valid timezone."
