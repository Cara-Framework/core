from datetime import datetime
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class DateFormatRule(BaseRule):
    """
    Validate that a value matches a given datetime format.

    Usage: "dateformat:%Y-%m-%d"  (defaults to %Y-%m-%d if no param given)
    """

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        fmt = params.get("dateformat") or "%Y-%m-%d"
        if value is None:
            return False
        try:
            datetime.strptime(str(value), fmt)
            return True
        except Exception:
            return False

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attribute = MessageFormatter.format_attribute_name(field)
        fmt = params.get("dateformat") or "%Y-%m-%d"
        return f"The {attribute.lower()} must match format {fmt}."
