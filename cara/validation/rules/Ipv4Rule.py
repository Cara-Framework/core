"""IPv4-only rule. Usage: ``ipv4``."""

from __future__ import annotations

import ipaddress
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class Ipv4Rule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None:
            return False
        try:
            ipaddress.IPv4Address(str(value))
            return True
        except ValueError:
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be a valid IPv4 address."
