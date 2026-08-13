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
    # The algorithm every caller gets unless it names another one. Policy
    # code needs this name to reason about the storage algorithm's limits,
    # so it is a constant rather than a literal repeated in each signature.
    DEFAULT_ALGORITHM = "argon2id"

    drivers = {
        "argon2id": Argon2idHasher(),
        "bcrypt": BcryptHasher(),
        "sha256": Sha256Hasher(),
    }

    #: A stored hash names its own algorithm in its prefix. Verification
    #: follows the ARTIFACT, not the write-side default — otherwise the
    #: day the default moves (bcrypt → argon2id), every legacy hash
    #: silently stops verifying and every existing user is locked out.
    _FORMAT_PREFIXES = (
        ("$argon2", "argon2id"),
        ("$2a$", "bcrypt"),
        ("$2b$", "bcrypt"),
        ("$2y$", "bcrypt"),
    )

    @classmethod
    def detect_algorithm(cls, hashed: str) -> str | None:
        """Name the algorithm a stored hash was made with, if recognizable."""
        if not isinstance(hashed, str):
            return None
        for prefix, name in cls._FORMAT_PREFIXES:
            if hashed.startswith(prefix):
                return name
        return None

    @classmethod
    def truncation_boundary(cls, algorithm: str = DEFAULT_ALGORITHM) -> int | None:
        """Bytes after which ``algorithm`` stops reading its input, else None.

        bcrypt authenticates only its first 72 bytes; longer passwords form a
        suffix-equivalence class. Password policy has to know this to keep a
        product from accepting a plaintext its own storage cannot round-trip,
        so the number is published by the driver rather than re-remembered by
        every caller.
        """
        driver = cls.drivers.get(algorithm)
        if not driver:
            raise InvalidArgumentException(f"Unsupported algorithm: {algorithm}")
        return getattr(driver, "TRUNCATES_AT_BYTES", None)

    @classmethod
    def make(
        cls,
        value: str,
        algorithm: str = DEFAULT_ALGORITHM,
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
        algorithm: str | None = None,
    ) -> bool:
        """Verify ``value`` against ``hashed``.

        The stored hash's own format wins when no algorithm is named:
        the write-side default only governs :meth:`make`, so moving it
        never strands hashes made under the previous one.
        """
        resolved = algorithm or cls.detect_algorithm(hashed) or cls.DEFAULT_ALGORITHM
        driver = cls.drivers.get(resolved)
        if not driver:
            raise InvalidArgumentException(f"Unsupported algorithm: {resolved}")
        return driver.check(value, hashed)

    @classmethod
    def needs_rehash(
        cls,
        hashed: str,
        algorithm: str = DEFAULT_ALGORITHM,
        rounds: int = 12,
    ) -> bool:
        driver = cls.drivers.get(algorithm)
        if not driver:
            raise InvalidArgumentException(f"Unsupported algorithm: {algorithm}")
        if algorithm == "bcrypt":
            return driver.needs_rehash(hashed, rounds)
        return driver.needs_rehash(hashed)
