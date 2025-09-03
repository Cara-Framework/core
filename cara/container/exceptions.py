"""
Container package exceptions.
"""

from cara.exceptions.types.base import CaraException

class RequiredContainerBindingNotFound(CaraException):
    """Thrown when a mandatory binding is missing."""

    pass

