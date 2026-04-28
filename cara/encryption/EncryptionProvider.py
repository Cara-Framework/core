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
        app_key = config("application.key") or config("encryption.key")
        if not app_key:
            raise EncryptionException("Application key is not set in config")

        algorithm = config("encryption.hash_algorithm", "sha256")
        cipher = config("encryption.cipher", "AES")

        self.application.bind("hash", lambda: Hash(algorithm=algorithm))
        self.application.bind("crypt", lambda: Crypt(key=app_key, cipher=cipher))
