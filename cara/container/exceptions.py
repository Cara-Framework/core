"""
Container package exceptions.
"""

from __future__ import annotations

from cara.exceptions.types.base import CaraException


class RequiredContainerBindingNotFound(CaraException):
    """Thrown when a mandatory binding is missing."""

    pass
