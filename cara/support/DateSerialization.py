"""Shared ISO-8601 datetime serialization helper."""

from __future__ import annotations

from typing import Any


def iso_datetime(value: Any) -> str | None:
    """Serialize a datetime-like value to an ISO-8601 string.

    Returns ``None`` when *value* is ``None``, calls ``.isoformat()``
    when available, and falls back to ``str(value)`` for anything else.
    """
    if value is None:
        return None
    try:
        return value.isoformat()
    except AttributeError:
        return str(value)
