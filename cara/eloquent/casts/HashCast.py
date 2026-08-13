"""Canonical definition of ``HashCast``."""

from __future__ import annotations

import hashlib
from typing import Any

import bcrypt

from cara.exceptions import InvalidArgumentException

from .BaseCast import BaseCast


class HashCast(BaseCast):
    """Cast for hashed values (passwords, etc.).

    Writes the value through a one-way hash; reads pass the hash through
    untouched, because hashed values cannot be reversed by design.
    """

    ALGORITHMS = {
        "bcrypt": "_hash_bcrypt",
        "sha256": "_hash_sha256",
        "sha512": "_hash_sha512",
    }

    def __init__(self, algorithm: str = "bcrypt"):
        self.algorithm = algorithm.lower()
        if self.algorithm not in self.ALGORITHMS:
            raise InvalidArgumentException(
                f"Unknown hash algorithm '{algorithm}'. "
                f"Supported: {sorted(self.ALGORITHMS)}"
            )

    def get(self, value: Any) -> Any:
        """Return the hash as-is; hashes are one-way."""
        return value

    def set(self, value: Any) -> str | None:
        """Hash the value using the configured algorithm."""
        if value is None:
            return None

        hash_method = self.ALGORITHMS[self.algorithm]
        return getattr(self, hash_method)(value)

    def _hash_bcrypt(self, value: Any) -> str:
        """Hash using bcrypt (recommended for passwords)."""

        payload = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        return bcrypt.hashpw(payload, bcrypt.gensalt()).decode("utf-8")

    def _hash_sha256(self, value: Any) -> str:
        return hashlib.sha256(str(value).encode("utf-8")).hexdigest()

    def _hash_sha512(self, value: Any) -> str:
        return hashlib.sha512(str(value).encode("utf-8")).hexdigest()
