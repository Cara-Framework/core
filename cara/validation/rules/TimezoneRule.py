"""Timezone rule (IANA name, e.g. ``Europe/Istanbul``). Usage: ``timezone``."""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule

try:
    from zoneinfo import ZoneInfo, available_timezones
except ImportError:  # py < 3.9 fallback (shouldn't hit in this codebase)
    ZoneInfo = None  # type: ignore
    available_timezones = lambda: set()  # type: ignore


class TimezoneRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        if not isinstance(value, str) or ZoneInfo is None:
            return False
        try:
            ZoneInfo(value)
            return True
        except Exception:
            return False

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be a valid timezone."
