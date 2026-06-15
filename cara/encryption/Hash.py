"""
Hash Utility for the Cara framework.

This module provides the Hash class, which offers a unified interface for password hashing and
verification using multiple algorithms (bcrypt, sha256).
"""

from __future__ import annotations

from cara.encryption.drivers import BcryptHasher, Sha256Hasher

# An unsupported ``algorithm`` is a bad ARGUMENT, not an encryption-operation
# failure — so it raises ``InvalidArgumentException`` (a ``ValueError``
# subclass), not ``EncryptionException`` (which is reserved for genuine
# cipher/key failures). Callers that validate inputs catch it as ``ValueError``.
from cara.exceptions import InvalidArgumentException


class Hash:
    drivers = {
        "bcrypt": BcryptHasher(),
        "sha256": Sha256Hasher(),
    }

    @classmethod
    def make(
        cls,
        value: str,
        algorithm: str = "bcrypt",
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
        algorithm: str = "bcrypt",
    ) -> bool:
        # Auto-detect bcrypt hashes so callers don't need to know
        # which algorithm was originally used. Bcrypt hashes always
        # start with ``$2b$`` (or ``$2a$`` / ``$2y$``).
        if hashed and hashed.startswith(("$2b$", "$2a$", "$2y$")):
            algorithm = "bcrypt"
        driver = cls.drivers.get(algorithm)
        if not driver:
            raise InvalidArgumentException(f"Unsupported algorithm: {algorithm}")
        return driver.check(value, hashed)

    @classmethod
    def needs_rehash(
        cls,
        hashed: str,
        algorithm: str = "bcrypt",
        rounds: int = 12,
    ) -> bool:
        driver = cls.drivers.get(algorithm)
        if not driver:
            raise InvalidArgumentException(f"Unsupported algorithm: {algorithm}")
        if algorithm == "bcrypt":
            return driver.needs_rehash(hashed, rounds)
        return driver.needs_rehash(hashed)
