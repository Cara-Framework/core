"""Date rule (parses ISO-style date/datetime strings). Usage: ``date``."""
from datetime import date, datetime
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


def _parse_date(value: Any):
    if isinstance(value, (date, datetime)):
        return value
    if not isinstance(value, str):
        return None
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


class DateRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        return _parse_date(value) is not None

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} is not a valid date."
