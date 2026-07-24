"""Not-regex rule. Usage: ``not_regex:/foo/``."""

from __future__ import annotations

import re
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class NotRegexRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        raw = params.get("not_regex")
        if not raw or value is None:
            return True
        try:
            return re.search(raw, str(value)) is None
        except re.error:
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} format is invalid."
