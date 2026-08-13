"""Validation for caller-supplied, origin-relative return destinations."""

from __future__ import annotations

from urllib.parse import unquote

_MAX_LENGTH = 512


def _shape_is_internal(value: str) -> bool:
    if not value.startswith("/"):
        return False
    if value.startswith("//") or "\\" in value:
        return False
    return all(ch > "\x20" and ch != "\x7f" for ch in value)


def _decodes_clean(value: str) -> bool:
    decoded = unquote(value)
    return decoded == value or _shape_is_internal(decoded)


def _is_safe(value: object) -> bool:
    """Whether ``value`` is an internal path safe to navigate to."""
    return (
        isinstance(value, str)
        and bool(value)
        and len(value) <= _MAX_LENGTH
        and _shape_is_internal(value)
        and _decodes_clean(value)
    )


def _safe(value: object, fallback: str = "/") -> str:
    """Return a proven internal path, otherwise the caller's fallback."""
    return value if isinstance(value, str) and _is_safe(value) else fallback


class ReturnPath:
    """Namespace form for call sites that read better qualified."""

    MAX_LENGTH = _MAX_LENGTH

    is_safe = staticmethod(_is_safe)
    safe = staticmethod(_safe)
