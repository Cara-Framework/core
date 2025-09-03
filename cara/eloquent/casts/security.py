"""
Security Cast Types for Cara ORM

Provides hashing and encryption capabilities for sensitive data.
"""

import hashlib

from .base import BaseCast


class HashCast(BaseCast):
    """Cast for hashed values (passwords, etc.)."""

    ALGORITHMS = {
        "bcrypt": "_hash_bcrypt",
        "sha256": "_hash_sha256",
        "sha512": "_hash_sha512",
        "md5": "_hash_md5",
    }

    def __init__(self, algorithm: str = "bcrypt"):
        self.algorithm = algorithm.lower()

    def get(self, value):
        """Always return the hash (never unhash for security)."""
        return value

    def set(self, value):
        """Hash the value using specified algorithm."""
        if value is None:
            return None

        hash_method = self.ALGORITHMS.get(self.algorithm, "_hash_sha256")
        return getattr(self, hash_method)(value)

    def _hash_bcrypt(self, value):
        """Hash using bcrypt (recommended for passwords)."""
        try:
            import bcrypt

            if isinstance(value, str):
                value = value.encode("utf-8")
            return bcrypt.hashpw(value, bcrypt.gensalt()).decode("utf-8")
        except ImportError:
            # Fallback to SHA256 if bcrypt not available
            return self._hash_sha256(value)

    def _hash_sha256(self, value):
        """Hash using SHA256."""
        return hashlib.sha256(str(value).encode("utf-8")).hexdigest()

    def _hash_sha512(self, value):
        """Hash using SHA512."""
        return hashlib.sha512(str(value).encode("utf-8")).hexdigest()

    def _hash_md5(self, value):
        """Hash using MD5 (not recommended for passwords)."""
        return hashlib.md5(str(value).encode("utf-8")).hexdigest()


class EncryptedCast(BaseCast):
    """
    Cast for encrypted field values.

    Note: This is a placeholder implementation.
    In production, integrate with a proper encryption service.
    """

    def __init__(self, key: str = None):
        self.key = key or "default-encryption-key"  # Should be from config

    def get(self, value):
        """Decrypt value."""
        if value is None:
            return None

        # TODO: Implement actual decryption
        # This is a placeholder - DO NOT USE IN PRODUCTION
        try:
            return self._simple_decrypt(value)
        except Exception:
            return value

    def set(self, value):
        """Encrypt value."""
        if value is None:
            return None

        # TODO: Implement actual encryption
        # This is a placeholder - DO NOT USE IN PRODUCTION
        try:
            return self._simple_encrypt(str(value))
        except Exception:
            return str(value)

    def _simple_encrypt(self, value):
        """
        Simple XOR encryption (PLACEHOLDER ONLY).

        WARNING: This is NOT secure! Use proper encryption in production.
        """
        key_bytes = self.key.encode("utf-8")
        value_bytes = value.encode("utf-8")

        encrypted = bytearray()
        for i, byte in enumerate(value_bytes):
            encrypted.append(byte ^ key_bytes[i % len(key_bytes)])

        return encrypted.hex()

    def _simple_decrypt(self, encrypted_hex):
        """
        Simple XOR decryption (PLACEHOLDER ONLY).

        WARNING: This is NOT secure! Use proper encryption in production.
        """
        key_bytes = self.key.encode("utf-8")
        encrypted_bytes = bytes.fromhex(encrypted_hex)

        decrypted = bytearray()
        for i, byte in enumerate(encrypted_bytes):
            decrypted.append(byte ^ key_bytes[i % len(key_bytes)])

        return decrypted.decode("utf-8")


class TokenCast(BaseCast):
    """Cast for generating and validating tokens."""

    def __init__(self, length: int = 32):
        self.length = length

    def get(self, value):
        """Return token as-is."""
        return value

    def set(self, value):
        """Generate token if value is None, otherwise return value."""
        if value is None:
            return self._generate_token()
        return str(value)

    def _generate_token(self):
        """Generate a random token."""
        import secrets

        return secrets.token_urlsafe(self.length)
