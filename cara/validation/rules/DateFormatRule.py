from __future__ import annotations

from datetime import datetime
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class DateFormatRule(BaseRule):
    """
    Validate that a value matches a given datetime format.

    Usage: "date_format:%Y-%m-%d"  (defaults to %Y-%m-%d if no param given)
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        fmt = params.get("date_format") or "%Y-%m-%d"
        if value is None:
            return False
        try:
            datetime.strptime(str(value), fmt)
            return True
        except ValueError, TypeError, OverflowError:
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attribute = MessageFormatter.format_attribute_name(field)
        fmt = params.get("date_format") or "%Y-%m-%d"
        return f"The {attribute.lower()} must match format {fmt}."
