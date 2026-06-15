"""
Numeric Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is numeric.
"""

from __future__ import annotations

import math
import re
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


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

    STRING-SHAPE NOTE:
      Python's ``float()`` is also permissive about surrounding
      whitespace + PEP 515 underscore separators
      (``float("\\t3.14") == 3.14``, ``float("1_000.5") == 1000.5``).
      The raw string would otherwise leak into log lines / Prom
      labels / JSON responses unchanged. The strict pattern below
      requires the canonical form ``[+-]?digits[.digits][e±digits]``
      before the ``float()`` round-trip so newline-tainted values
      and underscore-separated values are rejected.
    """

    # Canonical decimal-with-optional-scientific shape. Anchored with
    # ``fullmatch`` to defeat the default-mode ``$``-allows-trailing-
    # newline quirk.
    _STRICT_NUMERIC_PATTERN = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$")

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
            # Strict canonical shape — Python ``float()`` is
            # permissive (``float("\t3.14") == 3.14``,
            # ``float("1_000.5") == 1000.5``) and the raw string
            # could leak downstream into log lines / Prom labels /
            # JSON responses with embedded control characters or
            # PEP 515 underscore separators no caller actually typed.
            # ``fullmatch`` removes the default-mode ``$`` trailing-
            # newline allowance the regex engine carries.
            if not self._STRICT_NUMERIC_PATTERN.fullmatch(value):
                return False
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
