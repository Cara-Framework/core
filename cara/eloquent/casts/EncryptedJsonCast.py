"""Canonical definition of ``EncryptedJsonCast``."""

from __future__ import annotations

import json
from typing import Any

from cara.exceptions import EncryptionException

from .EncryptedCast import EncryptedCast


class EncryptedJsonCast(EncryptedCast):
    """Encrypted-at-rest JSON for jsonb columns (credentials, secrets).

    The ciphertext rides a self-describing envelope ``{"$enc": token}``
    so the column stays valid jsonb. Plaintext or malformed envelopes are
    rejected; release migrations must transform every existing row before the
    new application image starts.
    """

    ENVELOPE_KEY = "$enc"

    @classmethod
    def token_of(cls, value: Any) -> str:
        """The ciphertext token inside a stored envelope (no decryption)."""
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (ValueError, TypeError) as exc:
                raise EncryptionException(
                    "Encrypted JSON value is not a valid envelope"
                ) from exc
        if not isinstance(value, dict) or set(value) != {cls.ENVELOPE_KEY}:
            raise EncryptionException("Encrypted JSON envelope is missing")
        return value[cls.ENVELOPE_KEY]

    def get(self, value: Any, context: str | None = None) -> Any:
        if value is None:
            return None
        token = self.token_of(value)
        return json.loads(self._cipher().decrypt(token, context=context))

    def set(self, value: Any, context: str | None = None) -> str | None:
        if value is None:
            return None
        token = self._cipher().encrypt(json.dumps(value), context=context)
        return json.dumps({self.ENVELOPE_KEY: token})
