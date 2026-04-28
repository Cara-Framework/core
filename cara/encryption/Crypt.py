"""
Symmetric Encryption Utility for the Cara framework.

Uses AES-256-GCM (authenticated encryption) so ciphertext tampering
is detected at decryption time.

Payload layout (base64-encoded):  nonce (12 bytes) || tag (16 bytes) || ciphertext
"""

import hashlib
from base64 import b64decode, b64encode

from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

from cara.exceptions import EncryptionException

_NONCE_LEN = 12
_TAG_LEN = 16


class Crypt:
    def __init__(self, key: str):
        self.key = hashlib.sha256(key.encode()).digest()

    def encrypt(self, value: str) -> str:
        try:
            nonce = get_random_bytes(_NONCE_LEN)
            cipher = AES.new(self.key, AES.MODE_GCM, nonce=nonce)
            ciphertext, tag = cipher.encrypt_and_digest(value.encode())
            return b64encode(nonce + tag + ciphertext).decode()
        except Exception as e:
            raise EncryptionException(f"Encryption failed: {e}") from e

    def decrypt(self, token: str) -> str:
        try:
            raw = b64decode(token)
            if len(raw) < _NONCE_LEN + _TAG_LEN:
                raise ValueError("Ciphertext too short")
            nonce = raw[:_NONCE_LEN]
            tag = raw[_NONCE_LEN : _NONCE_LEN + _TAG_LEN]
            ciphertext = raw[_NONCE_LEN + _TAG_LEN :]
            cipher = AES.new(self.key, AES.MODE_GCM, nonce=nonce)
            plaintext = cipher.decrypt_and_verify(ciphertext, tag)
            return plaintext.decode()
        except Exception as e:
            raise EncryptionException(f"Decryption failed: {e}") from e
