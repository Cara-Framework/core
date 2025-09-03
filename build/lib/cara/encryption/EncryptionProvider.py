"""
Encryption Provider for the Cara framework.

This module provides the service provider that registers the Hash and Crypt utilities, binding them
to the application container with the configured application key.
"""

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
        """Register encryption services with configuration."""
        app_settings = config("application", {})
        encryption_settings = config("encryption", {})

        # Validate application key
        app_key = app_settings.get("key") or encryption_settings.get("key")
        if not app_key:
            raise EncryptionException("Application key is not set in config")

        # Register encryption services
        self._add_hash_service(encryption_settings)
        self._add_crypt_service(app_key, encryption_settings)

    def _add_hash_service(self, settings: dict) -> None:
        """Register Hash service with configuration."""
        algorithm = settings.get("hash_algorithm", "sha256")
        self.application.bind("hash", lambda: Hash(algorithm=algorithm))

    def _add_crypt_service(self, app_key: str, settings: dict) -> None:
        """Register Crypt service with configuration."""
        cipher = settings.get("cipher", "AES")
        self.application.bind("crypt", lambda: Crypt(key=app_key, cipher=cipher))
