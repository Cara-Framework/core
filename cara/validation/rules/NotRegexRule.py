"""Not-regex rule. Usage: ``not_regex:/foo/``."""
import re
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class NotRegexRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        raw = params.get("not_regex") or params.get("notregex")
        if not raw or value is None:
            return True
        try:
            return re.search(raw, str(value)) is None
        except re.error:
            return False

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} format is invalid."
