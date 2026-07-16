"""
Encryption Provider for the Cara framework.

This module provides the service provider that registers the Hash and Crypt utilities, binding them
to the application container with the configured application key.
"""

from __future__ import annotations

from cara.configuration import config
from cara.encryption.Crypt import Crypt
from cara.encryption.Hash import Hash
from cara.exceptions import EncryptionException
from cara.foundation import DeferredProvider


class EncryptionProvider(DeferredProvider):
    """
    Deferred provider for the encryption subsystem.

    Reads configuration and registers Hash and Crypt services.
    """

    @classmethod
    def provides(cls) -> list[str]:
        return ["hash", "crypt"]

    def register(self):
        """Register hashing and the versioned encryption keyring."""
        keys = config("encryption.keys", {})
        current_key_id = config("encryption.current_key_id")
        if not keys or not current_key_id:
            raise EncryptionException("Encryption keyring is not configured")

        # Bind the ``Hash`` class itself so the ``Hash`` facade mirrors the
        # full class API: ``Hash.make(value)`` uses the secure Argon2id default
        # for passwords, while ``Hash.make(value, algorithm="sha256")`` yields a
        # deterministic digest for at-rest token storage, and ``Hash.check``
        # auto-detects the algorithm. ``Hash`` is all classmethods (no
        # ``__init__``), so the class *is* the service — the previous proxy
        # pinned a single algorithm (sha256), which would have silently weakened
        # any password hashed through the binding.
        self.application.bind("hash", lambda: Hash)
        self.application.bind(
            "crypt",
            lambda: Crypt(keys=keys, current_key_id=current_key_id),
        )
