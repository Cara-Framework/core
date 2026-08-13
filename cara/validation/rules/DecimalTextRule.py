"""Exact, non-negative decimal-text validation."""

from __future__ import annotations

from typing import Any

from cara.support import parse_decimal_text
from cara.validation.MessageFormatter import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class DecimalTextRule(BaseRule):
    """Validate ``decimal_text:<precision>,<scale>`` without float coercion."""

    @staticmethod
    def _shape(params: dict[str, Any]) -> tuple[int, int] | None:
        raw = params.get("decimal_text")
        try:
            precision_text, scale_text = str(raw).split(",", 1)
            precision, scale = int(precision_text), int(scale_text)
        except TypeError, ValueError:
            return None
        if precision < 1 or scale < 0 or scale > precision:
            return None
        return precision, scale

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        shape = self._shape(params)
        parsed = parse_decimal_text(value)
        if shape is None or parsed is None or parsed < 0:
            return False

        precision, scale = shape
        whole, separator, fraction = value.partition(".")
        if (separator and scale == 0) or len(fraction) > scale:
            return False
        integer_digits = 0 if whole == "0" else len(whole)
        return integer_digits <= precision - scale

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attribute = MessageFormatter.format_attribute_name(field).lower()
        shape = self._shape(params)
        if shape is None:
            return f"The {attribute} field has an invalid decimal_text rule."
        precision, scale = shape
        return (
            f"The {attribute} field must be non-negative decimal text with at most "
            f"{precision} digits and {scale} fractional digits."
        )


__all__ = ["DecimalTextRule"]
