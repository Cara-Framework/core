"""Strict signing-key validation."""

from __future__ import annotations

import hmac
import json
import re
from collections.abc import Mapping

_MIN_KEY_BYTES = 32
_KEY_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def require_independent_signing_key(
    *,
    value: str | bytes | None,
    variable_name: str,
    disallowed: Mapping[str, str | bytes | None] | None = None,
) -> str | bytes:
    """Require an explicit strong key independent from related secrets."""
    key = _key_bytes(value)
    _require_minimum(key, variable_name)
    for label, candidate in (disallowed or {}).items():
        other = _key_bytes(candidate)
        if other and hmac.compare_digest(key, other):
            raise RuntimeError(f"{variable_name} must be independent from {label}.")
    return value  # type: ignore[return-value]


def require_signing_keyring(
    *,
    active_key_id: str | None,
    active_key: str | bytes | None,
    previous_keys: str | Mapping[str, str | bytes] | None,
    disallowed: Mapping[str, str | bytes | None] | None = None,
) -> tuple[str, dict[str, str | bytes]]:
    """Validate an explicit active signing key plus a rotation keyring.

    ``previous_keys`` is a JSON object in environment-backed configuration and
    may be passed as a mapping by tests or programmatic callers. The active key
    is deliberately separate so rotation is an explicit two-step operation:
    add the old active key to the previous map, then replace the active id/key.
    """
    key_id = str(active_key_id or "").strip()
    if not _KEY_ID_PATTERN.fullmatch(key_id):
        raise RuntimeError(
            "QUEUE_SIGNING_KEY_ID must match "
            "^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$."
        )

    active = require_independent_signing_key(
        value=active_key,
        variable_name="QUEUE_SIGNING_KEY",
        disallowed=disallowed,
    )
    parsed = _parse_previous_keys(previous_keys)
    if key_id in parsed:
        raise RuntimeError(
            "QUEUE_SIGNING_PREVIOUS_KEYS must not repeat QUEUE_SIGNING_KEY_ID."
        )

    keyring: dict[str, str | bytes] = {key_id: active}
    seen: dict[str, bytes] = {key_id: _key_bytes(active)}
    forbidden = dict(disallowed or {})
    forbidden["QUEUE_SIGNING_KEY"] = active

    for previous_id, previous_key in parsed.items():
        if not _KEY_ID_PATTERN.fullmatch(previous_id):
            raise RuntimeError(
                "QUEUE_SIGNING_PREVIOUS_KEYS contains an invalid key id."
            )
        validated = require_independent_signing_key(
            value=previous_key,
            variable_name=f"QUEUE_SIGNING_PREVIOUS_KEYS[{previous_id!r}]",
            disallowed=forbidden,
        )
        value_bytes = _key_bytes(validated)
        for existing_id, existing_bytes in seen.items():
            if hmac.compare_digest(value_bytes, existing_bytes):
                raise RuntimeError(
                    "Queue signing keys must be distinct; "
                    f"{previous_id!r} duplicates {existing_id!r}."
                )
        keyring[previous_id] = validated
        seen[previous_id] = value_bytes
        forbidden[f"QUEUE_SIGNING_PREVIOUS_KEYS[{previous_id!r}]"] = validated

    return key_id, keyring


def _parse_previous_keys(
    value: str | Mapping[str, str | bytes] | None,
) -> dict[str, str | bytes]:
    if value is None:
        raise RuntimeError("QUEUE_SIGNING_PREVIOUS_KEYS must be an explicit JSON object.")
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "QUEUE_SIGNING_PREVIOUS_KEYS must be valid JSON."
            ) from exc
    elif isinstance(value, Mapping):
        parsed = dict(value)
    else:
        raise RuntimeError("QUEUE_SIGNING_PREVIOUS_KEYS must be a JSON object.")

    if not isinstance(parsed, dict):
        raise RuntimeError("QUEUE_SIGNING_PREVIOUS_KEYS must be a JSON object.")
    normalized: dict[str, str | bytes] = {}
    for key_id, key in parsed.items():
        if not isinstance(key_id, str) or not isinstance(key, (str, bytes)):
            raise RuntimeError(
                "QUEUE_SIGNING_PREVIOUS_KEYS must map string ids to string keys."
            )
        normalized[key_id] = key
    return normalized


def _key_bytes(value: str | bytes | None) -> bytes:
    if value is None:
        return b""
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, bytes):
        return value
    raise RuntimeError("Signing keys must be strings or bytes.")


def _require_minimum(value: bytes, label: str) -> None:
    if len(value) < _MIN_KEY_BYTES:
        raise RuntimeError(f"{label} must contain at least 32 bytes.")
