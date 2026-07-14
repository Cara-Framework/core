"""Argon2id password hasher using OWASP's baseline work factors."""

from __future__ import annotations

from argon2 import PasswordHasher, Type
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError


class Argon2idHasher:
    """Memory-hard password hashing; parameters are encoded in every hash."""

    def __init__(self) -> None:
        self._hasher = PasswordHasher(
            time_cost=2,
            memory_cost=19_456,
            parallelism=1,
            hash_len=32,
            salt_len=16,
            type=Type.ID,
        )

    def make(self, value: str) -> str:
        return self._hasher.hash(value)

    def check(self, value: str, hashed: str) -> bool:
        try:
            return bool(self._hasher.verify(hashed, value))
        except (VerifyMismatchError, VerificationError, InvalidHashError, TypeError):
            return False

    def needs_rehash(self, hashed: str) -> bool:
        try:
            return self._hasher.check_needs_rehash(hashed)
        except (InvalidHashError, TypeError):
            return True
