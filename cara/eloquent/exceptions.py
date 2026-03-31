"""
Eloquent package exceptions.
"""

from cara.exceptions.types.base import CaraException


class ConfigurationNotFound(CaraException):
    """Exception raised when a configuration is not found."""

    pass
