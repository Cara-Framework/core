"""
Encryption exception for the Cara framework.

This module defines exception types related to encryption operations.
"""

from __future__ import annotations

from .CaraException import CaraException


class EncryptionException(CaraException):
    pass


__all__ = [
    "EncryptionException",
]
