"""InvalidRuleFormatException."""

from __future__ import annotations

from .CaraException import CaraException


class InvalidRuleFormatException(CaraException):
    """Thrown if the rules dict is not in the expected format (e.g., not a dict of
    field→rule_string)."""

    pass
