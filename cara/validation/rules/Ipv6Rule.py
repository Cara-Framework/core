"""IPv6-only rule. Usage: ``ipv6``."""

import ipaddress
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class Ipv6Rule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None:
            return False
        try:
            ipaddress.IPv6Address(str(value))
            return True
        except ValueError:
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be a valid IPv6 address."
