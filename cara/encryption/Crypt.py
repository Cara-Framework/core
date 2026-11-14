"""
Symmetric Encryption Utility for the Cara framework.

Uses AES-256-GCM with a versioned key identifier so keys can rotate without
making older ciphertext unreadable.

Envelope: ``v2:<key-id>:<base64(nonce || tag || ciphertext)>``.

A second envelope exists for ciphertext that has to travel inside a URL:
``u2~<key-id>~<base64url(nonce || tag || ciphertext)>``. It is the same
construction with a transport-safe alphabet — ``~`` separates because it is
the one RFC 3986 unreserved character that appears in neither the key-id
pattern nor the base64url alphabet, so no percent-encoding can ever mangle
the envelope in a query string, a Location header, or an emailed link.

Binding (``context``): the associated data is the envelope header, plus the
caller's ``context`` when one is given. A model column declared in
``__seal_context__`` passes its row identity here (table, column, tenant,
row id), so a ciphertext copied onto another row — or another tenant —
refuses to open. The context is not stored: the reader must name the same
row to open it. ``context=None`` and a context are different keys in effect;
a value sealed one way never opens the other way.
"""

from __future__ import annotations

import hashlib
import os
import re
from base64 import b64decode, b64encode, urlsafe_b64decode, urlsafe_b64encode

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from cara.exceptions import EncryptionException

_NONCE_LEN = 12
_TAG_LEN = 16
_VERSION = "v2"
_URLSAFE_VERSION = "u2"
_URLSAFE_SEPARATOR = "~"
_KEY_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
# Unit separator: cannot appear in an envelope header, so ``header + ctx``
# never collides with another header.
_CONTEXT_SEPARATOR = b"\x1f"


def _associated_data(header: str, context: str | None) -> bytes:
    if context is None:
        return header.encode()
    if not isinstance(context, str) or not context:
        raise EncryptionException("Encryption context must be a non-empty string")
    return header.encode() + _CONTEXT_SEPARATOR + context.encode()


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

    def _seal(self, header: str, value: str, context: str | None) -> bytes:
        nonce = os.urandom(_NONCE_LEN)
        sealed = AESGCM(self.keys[self.current_key_id]).encrypt(
            nonce,
            value.encode(),
            _associated_data(header, context),
        )
        return nonce + sealed[-_TAG_LEN:] + sealed[:-_TAG_LEN]

    def _open(self, key_id: str, header: str, raw: bytes, context: str | None) -> str:
        key = self.keys.get(key_id)
        if key is None:
            raise EncryptionException(f"Encryption key is unavailable: {key_id}")
        if len(raw) < _NONCE_LEN + _TAG_LEN:
            raise EncryptionException("Ciphertext too short")
        nonce = raw[:_NONCE_LEN]
        tag = raw[_NONCE_LEN : _NONCE_LEN + _TAG_LEN]
        ciphertext = raw[_NONCE_LEN + _TAG_LEN :]
        try:
            plaintext = AESGCM(key).decrypt(
                nonce,
                ciphertext + tag,
                _associated_data(header, context),
            )
        except InvalidTag as e:
            # InvalidTag carries no message; say what it means instead of
            # surfacing "Decryption failed: " with nothing after it.
            bound = "for this row" if context is not None else "without a row binding"
            raise EncryptionException(
                f"Ciphertext sealed with key {key_id} does not open {bound}: it was "
                "altered, copied from another row, or sealed with a different binding "
                "(re-seal it with the credentials re-seal command)"
            ) from e
        return plaintext.decode()

    @staticmethod
    def key_id_of(token: str) -> str:
        """The key id a ``v2`` envelope was sealed with (no decryption)."""
        parts = str(token or "").split(":", 2)
        if len(parts) != 3 or parts[0] != _VERSION:
            raise EncryptionException("Unsupported ciphertext envelope")
        if not _KEY_ID_PATTERN.fullmatch(parts[1]):
            raise EncryptionException("Unsupported ciphertext envelope")
        return parts[1]

    def encrypt(self, value: str, *, context: str | None = None) -> str:
        try:
            header = f"{_VERSION}:{self.current_key_id}"
            payload = b64encode(self._seal(header, value, context)).decode()
            return f"{header}:{payload}"
        except EncryptionException:
            raise
        except Exception as e:
            raise EncryptionException(f"Encryption failed: {e}") from e

    def decrypt(self, token: str, *, context: str | None = None) -> str:
        try:
            parts = token.split(":", 2)
            if len(parts) != 3:
                raise EncryptionException("Unsupported ciphertext envelope")
            version, key_id, payload = parts
            if version != _VERSION or not _KEY_ID_PATTERN.fullmatch(key_id):
                raise EncryptionException("Unsupported ciphertext envelope")
            raw = b64decode(payload, validate=True)
            return self._open(key_id, f"{version}:{key_id}", raw, context)
        except EncryptionException:
            raise
        except Exception as e:
            raise EncryptionException(f"Decryption failed: {str(e) or type(e).__name__}") from e

    def encrypt_urlsafe(self, value: str, *, context: str | None = None) -> str:
        """Encrypt into an envelope that survives a URL unescaped.

        Same AES-256-GCM construction and same keyring as ``encrypt`` — only
        the transport alphabet differs. Padding is stripped because ``=`` is
        reserved in a query string; ``decrypt_urlsafe`` restores it.
        """
        try:
            header = f"{_URLSAFE_VERSION}{_URLSAFE_SEPARATOR}{self.current_key_id}"
            sealed = self._seal(header, value, context)
            payload = urlsafe_b64encode(sealed).decode().rstrip("=")
            return f"{header}{_URLSAFE_SEPARATOR}{payload}"
        except EncryptionException:
            raise
        except Exception as e:
            raise EncryptionException(f"Encryption failed: {e}") from e

    def decrypt_urlsafe(self, token: str, *, context: str | None = None) -> str:
        """Reverse ``encrypt_urlsafe``.

        The URL-safe envelope is deliberately NOT accepted by ``decrypt`` and
        vice versa: the version tag is part of the AEAD associated data, so a
        ciphertext minted for one transport cannot be replayed through the
        other even though both derive from the same key.
        """
        try:
            parts = token.split(_URLSAFE_SEPARATOR, 2)
            if len(parts) != 3:
                raise EncryptionException("Unsupported ciphertext envelope")
            version, key_id, payload = parts
            if version != _URLSAFE_VERSION or not _KEY_ID_PATTERN.fullmatch(key_id):
                raise EncryptionException("Unsupported ciphertext envelope")
            raw = urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
            header = f"{version}{_URLSAFE_SEPARATOR}{key_id}"
            return self._open(key_id, header, raw, context)
        except EncryptionException:
            raise
        except Exception as e:
            raise EncryptionException(f"Decryption failed: {str(e) or type(e).__name__}") from e
