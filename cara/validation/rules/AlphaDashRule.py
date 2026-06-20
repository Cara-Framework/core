"""Alpha-dash validation rule. Usage: ``alpha_dash``."""

from __future__ import annotations

import re
from typing import Any

from cara.validation.rules.BaseRule import BaseRule


class AlphaDashRule(BaseRule):
    """Validates that a value contains only alpha-numeric characters, dashes, and underscores."""

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None:
            return False
        if not isinstance(value, str):
            value = str(value)
        return bool(re.fullmatch(r"[a-zA-Z0-9_-]+", value))

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        return f"'{field}' must contain only alpha-numeric characters, dashes, and underscores."
