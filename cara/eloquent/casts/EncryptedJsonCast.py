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

    def get(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (ValueError, TypeError) as exc:
                raise EncryptionException(
                    "Encrypted JSON value is not a valid envelope"
                ) from exc
        if not isinstance(value, dict) or set(value) != {self.ENVELOPE_KEY}:
            raise EncryptionException("Encrypted JSON envelope is missing")
        return json.loads(self._cipher().decrypt(value[self.ENVELOPE_KEY]))

    def set(self, value: Any) -> str | None:
        if value is None:
            return None
        token = self._cipher().encrypt(json.dumps(value))
        return json.dumps({self.ENVELOPE_KEY: token})
