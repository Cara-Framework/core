"""IP address rule (v4 or v6). Usage: ``ip``."""

import ipaddress
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class IpRule(BaseRule):
    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None:
            return False
        try:
            ipaddress.ip_address(str(value))
            return True
        except ValueError:
            return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} must be a valid IP address."
