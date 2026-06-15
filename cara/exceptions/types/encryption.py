"""
Encryption Exception Type for the Cara framework.

This module defines exception types related to encryption operations.
"""

from __future__ import annotations

from .base import CaraException


class EncryptionException(CaraException):
    pass


__all__ = [
    "EncryptionException",
]
