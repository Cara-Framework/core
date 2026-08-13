"""
Missing Eloquent configuration exception.
"""

from __future__ import annotations

from cara.exceptions import CaraException


class ConfigurationNotFound(CaraException):
    """Exception raised when a configuration is not found."""

    pass
