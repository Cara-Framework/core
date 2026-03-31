"""
SHA-256 Hasher Driver for the Cara framework.

This module provides the Sha256Hasher class, which implements password hashing and verification
using the SHA-256 algorithm.
"""

import hashlib


class Sha256Hasher:
    def make(self, value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()

    def check(self, value: str, hashed: str) -> bool:
        return hashlib.sha256(value.encode()).hexdigest() == hashed

    def needs_rehash(self, hashed: str) -> bool:
        return False
