"""
Bcrypt Hasher Driver for the Cara framework.

This module provides the BcryptHasher class, which implements password hashing and verification
using the bcrypt algorithm.
"""

from __future__ import annotations

import bcrypt


class BcryptHasher:
    # bcrypt authenticates only the first 72 bytes of its input; everything
    # beyond is discarded by the algorithm itself. Declaring the boundary
    # here makes it readable by policy code (``cara.authentication``'s
    # password policy clamps its byte ceiling to it) instead of being a
    # literal repeated wherever someone remembers the limit exists.
    TRUNCATES_AT_BYTES = 72

    def make(self, value: str, rounds: int = 12) -> str:
        salt = bcrypt.gensalt(rounds)
        return bcrypt.hashpw(value.encode(), salt).decode()

    def check(self, value: str, hashed: str) -> bool:
        # Rejecting over-long inputs prevents suffix-equivalent passwords on
        # older bcrypt builds and normalizes bcrypt 5.x's ValueError into an
        # auth miss.
        if len(value.encode("utf-8")) > self.TRUNCATES_AT_BYTES:
            return False
        try:
            return bcrypt.checkpw(value.encode(), hashed.encode())
        except TypeError, ValueError:
            return False

    def needs_rehash(self, hashed: str, rounds: int = 12) -> bool:
        try:
            return int(hashed.split("$", 3)[2]) != int(rounds)
        except AttributeError, IndexError, TypeError, ValueError:
            return True
