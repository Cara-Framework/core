from __future__ import annotations

import re
from typing import Any

from cara.validation import MessageFormatter

from .BaseRule import BaseRule


class PhoneRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None or not isinstance(value, str):
            return False
        stripped_value = value.strip()
        return re.fullmatch(r"\+\d{7,15}", stripped_value) is not None

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attribute = MessageFormatter.format_attribute_name(field)
        return (
            f"The {attribute.lower()} field must be a valid phone number in E.164 format."
        )
