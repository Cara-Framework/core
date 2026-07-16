"""
Symmetric Encryption Utility for the Cara framework.

Uses AES-256-GCM with a versioned key identifier so keys can rotate without
making older ciphertext unreadable.

Envelope: ``v2:<key-id>:<base64(nonce || tag || ciphertext)>``.
"""

from __future__ import annotations

import hashlib
import os
import re
from base64 import b64decode, b64encode

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from cara.exceptions import EncryptionException

_NONCE_LEN = 12
_TAG_LEN = 16
_VERSION = "v2"
_KEY_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


class Crypt:
    def __init__(
        self,
        key: str | None = None,
        *,
        keys: dict[str, str] | None = None,
        current_key_id: str | None = None,
    ):
        if key is not None:
            keys = {"explicit": key}
            current_key_id = "explicit"
        if not keys or not current_key_id or current_key_id not in keys:
            raise EncryptionException(
                "Encryption keyring and current key id must be configured"
            )
        if not _KEY_ID_PATTERN.fullmatch(current_key_id):
            raise EncryptionException("Invalid encryption key id")
        self.current_key_id = current_key_id
        self.keys = {
            key_id: hashlib.sha256(secret.encode()).digest()
            for key_id, secret in keys.items()
            if _KEY_ID_PATTERN.fullmatch(key_id) and secret
        }
        if current_key_id not in self.keys:
            raise EncryptionException("Current encryption key is empty or invalid")

    def encrypt(self, value: str) -> str:
        try:
            nonce = os.urandom(_NONCE_LEN)
            header = f"{_VERSION}:{self.current_key_id}"
            sealed = AESGCM(self.keys[self.current_key_id]).encrypt(
                nonce,
                value.encode(),
                header.encode(),
            )
            ciphertext = sealed[:-_TAG_LEN]
            tag = sealed[-_TAG_LEN:]
            payload = b64encode(nonce + tag + ciphertext).decode()
            return f"{header}:{payload}"
        except EncryptionException:
            raise
        except Exception as e:
            raise EncryptionException(f"Encryption failed: {e}") from e

    def decrypt(self, token: str) -> str:
        try:
            parts = token.split(":", 2)
            if len(parts) != 3:
                raise EncryptionException("Unsupported ciphertext envelope")
            version, key_id, payload = parts
            if version != _VERSION or not _KEY_ID_PATTERN.fullmatch(key_id):
                raise EncryptionException("Unsupported ciphertext envelope")
            key = self.keys.get(key_id)
            if key is None:
                raise EncryptionException(f"Encryption key is unavailable: {key_id}")
            raw = b64decode(payload, validate=True)
            if len(raw) < _NONCE_LEN + _TAG_LEN:
                raise EncryptionException("Ciphertext too short")
            nonce = raw[:_NONCE_LEN]
            tag = raw[_NONCE_LEN : _NONCE_LEN + _TAG_LEN]
            ciphertext = raw[_NONCE_LEN + _TAG_LEN :]
            plaintext = AESGCM(key).decrypt(
                nonce,
                ciphertext + tag,
                f"{version}:{key_id}".encode(),
            )
            return plaintext.decode()
        except EncryptionException:
            raise
        except Exception as e:
            raise EncryptionException(f"Decryption failed: {e}") from e
