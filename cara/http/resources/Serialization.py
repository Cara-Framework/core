"""Shared serialization helpers for API resources.

Eliminates duplicated opt_* functions across JsonResource and BaseResource.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any


def opt_float(value: Any) -> float | None:
    """Coerce to float, preserving None."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def opt_int(value: Any) -> int | None:
    """Coerce to int, preserving None."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def opt_str(value: Any, default: str = "") -> str:
    """Coerce to string with a fallback default."""
    if value is None:
        return default
    return str(value).strip() or default


def opt_datetime(value: Any) -> str | None:
    """Coerce a datetime-like value to an ISO-8601 string, preserving None.

    Always emits an explicit timezone offset for datetime values: a
    naive ``datetime`` (no ``tzinfo``) is interpreted as UTC, which
    matches the codebase convention — the DB stores wall-clock UTC
    and the model layer round-trips through pendulum-in-UTC. Without
    the offset, frontend ``new Date(...)`` parses the string as
    browser-local time and two users in different timezones see
    different absolute moments for the same column.

    ``date`` instances (no time component) are returned as plain
    ``YYYY-MM-DD`` — they intentionally carry no time-of-day, so
    appending an offset would lie about precision.

    Datetime-shaped strings (e.g. ``"2026-05-23 12:30:45"`` from a
    raw ``DB.select`` row) are normalised to ISO 8601 with a UTC
    suffix; Safari historically rejects the space-separated form.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    s = str(value).strip() if value else None
    if not s:
        return None
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        try:
            date.fromisoformat(s)
            return s
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(s.replace(" ", "T", 1))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.isoformat()
    except ValueError:
        return s


def opt_bool(value: Any, default: bool = False) -> bool:
    """Coerce to bool with a fallback default."""
    if value is None:
        return default
    return bool(value)


def opt_list(value: Any) -> list | None:
    """Return list or None."""
    return list(value) if value else None
