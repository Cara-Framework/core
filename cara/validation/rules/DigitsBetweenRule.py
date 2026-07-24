"""Digits-between validation rule. Usage: ``digits_between:3,8``."""

from __future__ import annotations

import re
from typing import Any

from cara.validation.rules.BaseRule import BaseRule


class DigitsBetweenRule(BaseRule):
    """Validates that a value is numeric and has a digit count within a range."""

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        raw = params.get("digits_between")
        if value is None or raw is None:
            return False
        parts = str(raw).split(",")
        if len(parts) != 2:
            return False
        try:
            lo, hi = int(parts[0].strip()), int(parts[1].strip())
        except ValueError, TypeError:
            return False
        s = str(value)
        if not re.fullmatch(r"[0-9]+", s):
            return False
        return lo <= len(s) <= hi

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        return f"'{field}' must be between {params.get('digits_between', '?')} digits."
