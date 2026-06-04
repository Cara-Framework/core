"""
Encryption Provider for the Cara framework.

This module provides the service provider that registers the Hash and Crypt utilities, binding them
to the application container with the configured application key.
"""

from __future__ import annotations

from cara.configuration import config
from cara.encryption import Crypt, Hash
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
        """Register encryption services with configuration.

        Key lookup order:
          1. ``encryption.key`` — optional override for callers who
             want to isolate the encryption secret from app-wide
             ``APP_KEY`` rotation (rotating the session signing key
             shouldn't invalidate every encrypted column).
          2. ``app.key`` — canonical Laravel-style application secret
             populated from ``APP_KEY`` by every config/app.py.

        Pre-fix the provider looked up ``application.key`` first,
        which is not a real config path in any tree of this codebase
        (no ``application.py`` config file exists). That meant the
        provider always raised "Application key is not set in config"
        even when ``APP_KEY`` was set — silently breaking the moment
        anyone added an ``EncryptionCast``-backed column. Matches the
        path ``FileCacheDriver`` already reads from (see
        ``cara/cache/drivers/FileCacheDriver.py``).
        """
        app_key = config("encryption.key") or config("app.key")
        if not app_key:
            raise EncryptionException("Application key is not set in config")

        algorithm = config("encryption.hash_algorithm", "sha256")

        self.application.bind("hash", lambda: Hash(algorithm=algorithm))
        # ``Crypt`` accepts only ``key`` — pre-fix the provider also
        # passed ``cipher=`` here which raised ``TypeError`` the moment
        # the factory was actually invoked. AES-GCM is the only mode
        # the implementation supports today; the parameter was vestigial.
        self.application.bind("crypt", lambda: Crypt(key=app_key))
