"""
Bcrypt Hasher Driver for the Cara framework.

This module provides the BcryptHasher class, which implements password hashing and verification
using the bcrypt algorithm.
"""

import bcrypt


class BcryptHasher:
    def make(self, value: str, rounds: int = 12) -> str:
        salt = bcrypt.gensalt(rounds)
        return bcrypt.hashpw(value.encode(), salt).decode()

    def check(self, value: str, hashed: str) -> bool:
        return bcrypt.checkpw(value.encode(), hashed.encode())

    def needs_rehash(self, hashed: str, rounds: int = 12) -> bool:
        return False
