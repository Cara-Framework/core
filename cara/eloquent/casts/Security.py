"""
Security Cast Types for Cara ORM.

Provides hashing and encryption capabilities for sensitive data.
Encryption delegates to the ``Crypt`` facade (AES-CBC) registered in the
container by :class:`EncryptionProvider`.
"""

from __future__ import annotations

import hashlib
from typing import Any

from cara.exceptions import EncryptionException, InvalidArgumentException

from .base import BaseCast


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
        try:
            import bcrypt
        except ImportError as exc:
            raise EncryptionException(
                "bcrypt is required for HashCast(algorithm='bcrypt'). "
                "Install it with `pip install bcrypt`."
            ) from exc

        payload = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        return bcrypt.hashpw(payload, bcrypt.gensalt()).decode("utf-8")

    def _hash_sha256(self, value: Any) -> str:
        return hashlib.sha256(str(value).encode("utf-8")).hexdigest()

    def _hash_sha512(self, value: Any) -> str:
        return hashlib.sha512(str(value).encode("utf-8")).hexdigest()


class EncryptedCast(BaseCast):
    """
    Cast for encrypted field values.

    Uses the :class:`cara.facades.Crypt` facade, which is backed by the
    framework's AES-CBC :class:`~cara.encryption.Crypt` implementation.
    Values are encrypted on write and decrypted on read.
    """

    def __init__(self, key: str | None = None):
        # ``key`` is accepted for Laravel parity (``__casts__ = {"field": "encrypted:my_key"}``)
        # and, when provided, creates an ad-hoc Crypt instance instead of the
        # container-bound default. When None, the bound Crypt facade is used.
        self._explicit_key = key

    def _cipher(self):
        if self._explicit_key is not None:
            from cara.encryption.Crypt import Crypt as CryptImpl

            return CryptImpl(self._explicit_key)

        from cara.facades import Crypt

        return Crypt

    def get(self, value: Any) -> Any:
        """Decrypt the stored value."""
        if value is None:
            return None
        return self._cipher().decrypt(value)

    def set(self, value: Any) -> str | None:
        """Encrypt the value before persisting."""
        if value is None:
            return None
        return self._cipher().encrypt(str(value))


class EncryptedJsonCast(EncryptedCast):
    """Encrypted-at-rest JSON for jsonb columns (credentials, secrets).

    The ciphertext rides a self-describing envelope ``{"$enc": token}``
    so the column stays valid jsonb. The reader is tolerant on purpose:
    a legacy plaintext dict (pre-encryption rows) is returned as-is and
    becomes encrypted on its next save — migration by attrition, no
    downtime re-encrypt pass required.
    """

    ENVELOPE_KEY = "$enc"

    def get(self, value: Any) -> Any:
        import json

        if value is None:
            return None
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (ValueError, TypeError):
                return None
        if isinstance(value, dict) and self.ENVELOPE_KEY in value:
            try:
                return json.loads(self._cipher().decrypt(value[self.ENVELOPE_KEY]))
            except Exception:
                return None  # wrong APP_KEY / corrupt token — never leak the envelope
        return value  # legacy plaintext row

    def set(self, value: Any) -> str | None:
        import json

        if value is None:
            return None
        token = self._cipher().encrypt(json.dumps(value))
        return json.dumps({self.ENVELOPE_KEY: token})


class TokenCast(BaseCast):
    """Cast for generating and validating tokens."""

    def __init__(self, length: int = 32):
        if length <= 0:
            raise InvalidArgumentException("Token length must be positive")
        self.length = length

    def get(self, value: Any) -> Any:
        """Return token as-is."""
        return value

    def set(self, value: Any) -> str:
        """Generate a token when the value is None, otherwise coerce to str."""
        if value is None:
            return self._generate_token()
        return str(value)

    def _generate_token(self) -> str:
        import secrets

        return secrets.token_urlsafe(self.length)
