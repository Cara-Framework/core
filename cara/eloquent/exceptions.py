"""
Eloquent package exceptions.
"""

from __future__ import annotations

from cara.exceptions.types.base import CaraException


class ConfigurationNotFound(CaraException):
    """Exception raised when a configuration is not found."""

    pass
