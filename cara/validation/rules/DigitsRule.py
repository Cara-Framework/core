"""Digits validation rule. Usage: ``digits:5``."""

from __future__ import annotations

import re
from typing import Any

from cara.validation.rules.BaseRule import BaseRule


class DigitsRule(BaseRule):
    """Validates that a value is numeric and has exactly *n* digits."""

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        length = params.get("digits")
        if value is None or length is None:
            return False
        s = str(value)
        try:
            expected = int(length)
        except ValueError, TypeError:
            return False
        return bool(re.fullmatch(r"[0-9]+", s)) and len(s) == expected

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        return f"'{field}' must be {params.get('digits', '?')} digits."
