"""Signed, context-bound keyset pagination cursors.

Clients must treat cursors as opaque.  A cursor carries the last visible
row's sort value plus canonical id, the sort direction, and a fingerprint of
the filtered result set.  The whole payload is authenticated with HMAC-SHA256
using ``APP_KEY``.

Unsigned base64 JSON is not an opaque cursor: callers can edit it, reuse it
against another endpoint/tenant/filter set, or feed an invalid comparand into
the database.  This codec therefore validates every claim and fails closed by
raising :class:`InvalidCursor`.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

SortDirection = Literal["asc", "desc"]
CursorPayload = dict[str, Any]

_VERSION = 1
_MAX_TOKEN_LENGTH = 4096
_PAYLOAD_KEYS = frozenset({"ver", "v", "id", "dir", "fp", "scope"})
_KEY_DERIVATION_LABEL = b"cara.cursor.v1"
_BASE64URL_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
)


class InvalidCursor(ValueError):
    """The cursor is malformed, tampered with, or belongs to another query."""


def cursor_fingerprint(filters: Any) -> str:
    """Return a deterministic SHA-256 fingerprint for query context.

    Callers should include every value that changes the visible result set,
    including tenant/user/channel permission scope.  Collections are
    normalized so semantically identical filter dictionaries produce the same
    fingerprint regardless of insertion order.
    """

    blob = json.dumps(
        _json_value(filters, nested=True),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def encode_cursor(
    sort_value: Any,
    primary_key: int | str,
    *,
    direction: SortDirection,
    fingerprint: str,
    scope: str,
    secret: str | bytes | None = None,
) -> str:
    """Authenticate a stable composite cursor for the last visible row."""

    payload: CursorPayload = {
        "ver": _VERSION,
        "v": _json_value(sort_value),
        "id": _primary_key(primary_key),
        "dir": _direction(direction),
        "fp": _fingerprint(fingerprint),
        "scope": _scope(scope),
    }
    body = _b64encode(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    )
    signature = _b64encode(
        hmac.new(_key(secret), body.encode("ascii"), hashlib.sha256).digest()
    )
    token = f"{body}.{signature}"
    if len(token) > _MAX_TOKEN_LENGTH:
        raise ValueError("Cursor payload exceeds the maximum token length.")
    return token


def decode_cursor(
    token: str,
    *,
    direction: SortDirection,
    fingerprint: str,
    scope: str,
    secret: str | bytes | None = None,
) -> CursorPayload:
    """Verify and decode a cursor, raising :class:`InvalidCursor` on failure."""

    if (
        not isinstance(token, str)
        or not token
        or len(token) > _MAX_TOKEN_LENGTH
        or token.count(".") != 1
    ):
        raise InvalidCursor("Invalid cursor.")

    body, supplied_signature = token.split(".", 1)
    try:
        body_bytes = body.encode("ascii")
        signature = _b64decode(supplied_signature)
    except (UnicodeEncodeError, ValueError) as exc:
        raise InvalidCursor("Invalid cursor.") from exc
    expected = hmac.new(_key(secret), body_bytes, hashlib.sha256).digest()
    if not hmac.compare_digest(signature, expected):
        raise InvalidCursor("Invalid cursor.")

    try:
        payload = json.loads(_b64decode(body).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidCursor("Invalid cursor.") from exc
    if not isinstance(payload, dict) or set(payload) != _PAYLOAD_KEYS:
        raise InvalidCursor("Invalid cursor.")

    try:
        payload_direction = _direction(payload.get("dir"))
        payload_fingerprint = _fingerprint(payload.get("fp"))
        payload_scope = _scope(payload.get("scope"))
        payload_id = _primary_key(payload.get("id"))
        payload_value = _json_value(payload.get("v"))
    except (TypeError, ValueError) as exc:
        raise InvalidCursor("Invalid cursor.") from exc

    if payload.get("ver") != _VERSION:
        raise InvalidCursor("Invalid cursor.")
    if payload_direction != _direction(direction):
        raise InvalidCursor("Cursor sort direction does not match this query.")
    if not hmac.compare_digest(payload_fingerprint, _fingerprint(fingerprint)):
        raise InvalidCursor("Cursor filters do not match this query.")
    if not hmac.compare_digest(payload_scope, _scope(scope)):
        raise InvalidCursor("Cursor does not belong to this endpoint.")

    return {
        "ver": _VERSION,
        "v": payload_value,
        "id": payload_id,
        "dir": payload_direction,
        "fp": payload_fingerprint,
        "scope": payload_scope,
    }


def slice_page_with_lookahead(
    rows: list[Any],
    limit: int,
    *,
    sort_field: str,
    direction: SortDirection,
    fingerprint: str,
    scope: str,
    primary_key: str = "id",
    secret: str | bytes | None = None,
) -> tuple[list[Any], str | None, bool]:
    """Trim a ``LIMIT limit + 1`` result and mint the next signed cursor."""

    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise ValueError("limit must be an integer between 1 and 100")
    has_more = len(rows) > limit
    visible = rows[:limit]
    if not visible or not has_more:
        return visible, None, False

    last = visible[-1]
    sort_value = _extract_field(last, sort_field)
    pk_value = _extract_field(last, primary_key)
    if pk_value is None:
        raise ValueError("Cursor page row is missing its canonical id.")
    return (
        visible,
        encode_cursor(
            sort_value,
            pk_value,
            direction=direction,
            fingerprint=fingerprint,
            scope=scope,
            secret=secret,
        ),
        True,
    )


def cursor_rules(*, max_limit: int = 100, min_limit: int = 1) -> dict[str, str]:
    """Strict request rules for cursor-paginated endpoints.

    ``page`` and ``offset`` are prohibited deliberately: silently accepting an
    obsolete deep-offset contract makes rollout bugs look like valid first-page
    reads.
    """

    if min_limit < 1 or max_limit > 100 or min_limit > max_limit:
        raise ValueError("cursor pagination limits must satisfy 1 <= min <= max <= 100")
    return {
        "limit": f"nullable|integer|between:{min_limit},{max_limit}",
        "cursor": f"bail|sometimes|required|string|max:{_MAX_TOKEN_LENGTH}",
        "page": "missing",
        "offset": "missing",
    }


def _key(secret: str | bytes | None) -> bytes:
    if secret is None:
        try:
            from cara.configuration import config

            secret = config("app.key", "") or os.environ.get("APP_KEY", "")
        except Exception:
            secret = os.environ.get("APP_KEY", "")
    raw = secret if isinstance(secret, bytes) else str(secret or "").encode("utf-8")
    if len(raw) < 32:
        raise RuntimeError("APP_KEY must contain at least 32 bytes for cursor signing.")
    return hmac.new(raw, _KEY_DERIVATION_LABEL, hashlib.sha256).digest()


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    if (
        not isinstance(value, str)
        or not value
        or any(char not in _BASE64URL_CHARS for char in value)
    ):
        raise ValueError("invalid base64url")
    try:
        raw = value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError("invalid base64url") from exc
    padded = raw + b"=" * (-len(raw) % 4)
    decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
    if _b64encode(decoded) != value:
        raise ValueError("non-canonical base64url")
    return decoded


def _direction(value: Any) -> SortDirection:
    if value not in {"asc", "desc"}:
        raise ValueError("direction must be 'asc' or 'desc'")
    return value


def _fingerprint(value: Any) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValueError("fingerprint must be lowercase SHA-256 hex")
    return value


def _scope(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 160:
        raise ValueError("scope must be a non-empty string of at most 160 characters")
    return value


def _primary_key(value: Any) -> int | str:
    if isinstance(value, bool):
        raise ValueError("cursor id cannot be boolean")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value and len(value) <= 255:
        return value
    raise ValueError("cursor id must be an integer or non-empty string")


def _json_value(value: Any, *, nested: bool = False) -> Any:
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool):
        if nested:
            return value
        raise ValueError("cursor sort value cannot be boolean")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("cursor values must be finite")
        return value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if nested and isinstance(value, dict):
        return {
            str(key): _json_value(item, nested=True)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if nested and isinstance(value, (list, tuple)):
        return [_json_value(item, nested=True) for item in value]
    if nested and isinstance(value, (set, frozenset)):
        normalized = [_json_value(item, nested=True) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        )
    raise ValueError(f"Unsupported cursor value type: {type(value).__name__}")


def _extract_field(row: Any, field: str) -> Any:
    if isinstance(row, dict):
        return row.get(field)
    return getattr(row, field, None)
