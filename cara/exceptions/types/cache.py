"""
Cache Exception Type for the Cara framework.

This module defines exception types related to cache operations.
"""

from __future__ import annotations

from .Base import CaraException


class CacheConfigurationException(CaraException):
    """Raised when required cache config is missing or invalid."""

    pass


# ``DriverNotRegisteredException`` lives in ``types.storage`` and nowhere
# else. This module used to declare its own copy; ``Cache.get_driver``
# raises the storage one (it imports from the ``cara.exceptions`` barrel,
# whose last-wins ordering picked storage), so an ``except`` clause written
# against the cache copy could never fire.


__all__ = [
    "CacheConfigurationException",
]
