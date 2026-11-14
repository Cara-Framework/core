"""Canonical definition of ``EncryptedCast``."""

from __future__ import annotations

from typing import Any

from cara.encryption import Crypt as CryptImpl
from cara.facades import Crypt

from .BaseCast import BaseCast


class EncryptedCast(BaseCast):
    """
    Cast for encrypted field values.

    Uses the :class:`cara.facades.Crypt` facade, which is backed by the
    framework's versioned AES-256-GCM keyring.
    Values are encrypted on write and decrypted on read.
    """

    def __init__(self, key: str | None = None):
        # ``key`` is accepted for Laravel parity (``__casts__ = {"field": "encrypted:my_key"}``)
        # and, when provided, creates an ad-hoc Crypt instance instead of the
        # container-bound default. When None, the bound Crypt facade is used.
        self._explicit_key = key

    def _cipher(self):
        if self._explicit_key is not None:
            return CryptImpl(self._explicit_key)

        return Crypt

    # The model passes a row context (``Model.__seal_context__``) to these
    # casts only; every other cast keeps the one-argument protocol.
    binds_row = True

    def get(self, value: Any, context: str | None = None) -> Any:
        """Decrypt the stored value (``context``: the row it is bound to)."""
        if value is None:
            return None
        return self._cipher().decrypt(value, context=context)

    def set(self, value: Any, context: str | None = None) -> str | None:
        """Encrypt the value before persisting (``context``: its row)."""
        if value is None:
            return None
        return self._cipher().encrypt(str(value), context=context)
