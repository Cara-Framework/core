"""
Numeric Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is numeric.
"""

import math
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class NumericRule(BaseRule):
    """Validates that a value is a *finite* numeric (int, float, or numeric string).

    ROOT-CAUSE NOTE (frontend_stress_log scenario 2, cycle 1):
      The previous implementation accepted ``Infinity`` / ``-Infinity`` /
      ``inf`` because Python's ``float()`` happily parses these tokens.
      Downstream filters then forwarded the infinite value into SQL,
      where Postgres treats ``Infinity`` as a sentinel that's strictly
      greater than every finite numeric column, so e.g.
      ``?price_max=Infinity`` silently became "no upper cap" and matched
      the whole catalog. ``NaN`` slipped through ``numeric`` and only
      got caught downstream by ``min:0`` (since ``NaN >= 0`` is False),
      which produced a confusing "must be at least 0" error message
      instead of the truthful "must be numeric".

      Tightened to require a finite value via ``math.isfinite``.
      ``Infinity`` / ``-Infinity`` / ``NaN`` now return a clean
      ``must be numeric`` error at the validation layer instead of
      leaking semantics-altering values into the filter SQL.
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        """Check if value is a finite numeric."""
        if value is None:
            return False

        if isinstance(value, bool):
            return False

        if isinstance(value, (int, float)):
            # Reject ``inf``, ``-inf``, ``nan`` regardless of how they
            # were spelled in the source — Python's ``float`` keeps the
            # non-finite identity through arithmetic, so ``isfinite``
            # is the canonical check.
            return math.isfinite(value)

        if isinstance(value, str):
            try:
                parsed = float(value)
            except ValueError:
                return False
            return math.isfinite(parsed)

        return False

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        """Return default numeric validation message."""
        attribute = MessageFormatter.format_attribute_name(field)
        return f"The {attribute.lower()} field must be numeric."
