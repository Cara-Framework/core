"""
Symmetric Encryption Utility for the Cara framework.

This module provides the Crypt class, which handles AES-based encryption and decryption of strings
using a key derived from SHA-256.
"""

import hashlib

from base64 import b64encode, b64decode
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from cara.exceptions import EncryptionException


class Crypt:
    def __init__(self, key: str):
        self.key = hashlib.sha256(key.encode()).digest()

    def encrypt(self, value: str) -> str:
        try:
            iv = get_random_bytes(16)
            cipher = AES.new(self.key, AES.MODE_CBC, iv)
            padded = self._pad(value.encode())
            encrypted = cipher.encrypt(padded)
            return b64encode(iv + encrypted).decode()
        except Exception as e:
            raise EncryptionException(f"Encryption failed: {e}")

    def decrypt(self, token: str) -> str:
        try:
            raw = b64decode(token)
            iv = raw[:16]
            encrypted = raw[16:]
            cipher = AES.new(self.key, AES.MODE_CBC, iv)
            padded = cipher.decrypt(encrypted)
            return self._unpad(padded).decode()
        except Exception as e:
            raise EncryptionException(f"Decryption failed: {e}")

    def _pad(self, b: bytes) -> bytes:
        pad_len = 16 - len(b) % 16
        return b + bytes([pad_len] * pad_len)

    def _unpad(self, b: bytes) -> bytes:
        pad_len = b[-1]
        return b[:-pad_len]
