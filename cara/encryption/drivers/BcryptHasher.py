"""
Bcrypt Hasher Driver for the Cara framework.

This module provides the BcryptHasher class, which implements password hashing and verification
using the bcrypt algorithm.
"""

from __future__ import annotations

import bcrypt


class BcryptHasher:
    def make(self, value: str, rounds: int = 12) -> str:
        salt = bcrypt.gensalt(rounds)
        return bcrypt.hashpw(value.encode(), salt).decode()

    def check(self, value: str, hashed: str) -> bool:
        # bcrypt only authenticates the first 72 bytes. Rejecting longer
        # inputs prevents suffix-equivalent passwords on older bcrypt builds
        # and normalizes bcrypt 5.x's ValueError into an auth miss.
        if len(value.encode("utf-8")) > 72:
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
