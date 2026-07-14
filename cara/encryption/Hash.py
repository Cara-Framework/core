"""
Hash Utility for the Cara framework.

This module provides the Hash class, which offers a unified interface for password hashing and
verification using multiple algorithms (Argon2id, bcrypt, sha256).
"""

from __future__ import annotations

from cara.encryption.drivers import Argon2idHasher, BcryptHasher, Sha256Hasher

# An unsupported ``algorithm`` is a bad ARGUMENT, not an encryption-operation
# failure — so it raises ``InvalidArgumentException`` (a ``ValueError``
# subclass), not ``EncryptionException`` (which is reserved for genuine
# cipher/key failures). Callers that validate inputs catch it as ``ValueError``.
from cara.exceptions import InvalidArgumentException


class Hash:
    drivers = {
        "argon2id": Argon2idHasher(),
        "bcrypt": BcryptHasher(),
        "sha256": Sha256Hasher(),
    }

    @classmethod
    def make(
        cls,
        value: str,
        algorithm: str = "argon2id",
        rounds: int = 12,
    ) -> str:
        driver = cls.drivers.get(algorithm)
        if not driver:
            raise InvalidArgumentException(f"Unsupported algorithm: {algorithm}")
        if algorithm == "bcrypt":
            return driver.make(value, rounds)
        return driver.make(value)

    @classmethod
    def check(
        cls,
        value: str,
        hashed: str,
        algorithm: str = "argon2id",
    ) -> bool:
        algorithm = cls._detect_algorithm(hashed, fallback=algorithm)
        driver = cls.drivers.get(algorithm)
        if not driver:
            raise InvalidArgumentException(f"Unsupported algorithm: {algorithm}")
        return driver.check(value, hashed)

    @classmethod
    def needs_rehash(
        cls,
        hashed: str,
        algorithm: str = "argon2id",
        rounds: int = 12,
    ) -> bool:
        stored_algorithm = cls._detect_algorithm(hashed, fallback=algorithm)
        if stored_algorithm != algorithm:
            return True
        driver = cls.drivers.get(stored_algorithm)
        if not driver:
            raise InvalidArgumentException(f"Unsupported algorithm: {algorithm}")
        if stored_algorithm == "bcrypt":
            return driver.needs_rehash(hashed, rounds)
        return driver.needs_rehash(hashed)

    @staticmethod
    def _detect_algorithm(hashed: str, *, fallback: str) -> str:
        if hashed and hashed.startswith("$argon2id$"):
            return "argon2id"
        if hashed and hashed.startswith(("$2b$", "$2a$", "$2y$")):
            return "bcrypt"
        return fallback
