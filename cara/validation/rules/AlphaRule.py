"""Alpha-only validation rule. Usage: ``alpha``."""

from __future__ import annotations

import re
from typing import Any

from cara.validation.rules.BaseRule import BaseRule


class AlphaRule(BaseRule):
    """Validates that a value contains only alphabetic characters (a-z, A-Z)."""

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None:
            return False
        if not isinstance(value, str):
            value = str(value)
        return bool(re.fullmatch(r"[a-zA-Z]+", value))

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        return f"'{field}' must contain only alphabetic characters."
