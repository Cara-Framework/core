"""
Hash Utility for the Cara framework.

This module provides the Hash class, which offers a unified interface for password hashing and
verification using multiple algorithms (bcrypt, sha256).
"""

from cara.encryption.drivers import BcryptHasher, Sha256Hasher


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
            raise ValueError(f"Unsupported algorithm: {algorithm}")
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
        driver = cls.drivers.get(algorithm)
        if not driver:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
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
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        if algorithm == "bcrypt":
            return driver.needs_rehash(hashed, rounds)
        return driver.needs_rehash(hashed)
