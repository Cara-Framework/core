"""RuleNotFoundException."""

from __future__ import annotations

from .CaraException import CaraException


class RuleNotFoundException(CaraException):
    """Thrown if a named rule does not exist in the rules map."""

    pass
