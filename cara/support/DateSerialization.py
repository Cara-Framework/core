"""Shared ISO-8601 datetime serialization helper."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any


def iso_datetime(value: Any) -> str | None:
    """Serialize a date or datetime-like value to canonical ISO-8601.

    Raw SQL adapters may return a timestamp string instead of ``datetime``;
    those values are parsed and normalized too. Naive datetimes are UTC by
    application convention so callers never ship browser-local ambiguity.
    Unknown non-date text remains unchanged rather than being guessed.
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

    raw = str(value).strip()
    if not raw:
        return None
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        try:
            return date.fromisoformat(raw).isoformat()
        except ValueError:
            return raw
    try:
        parsed = datetime.fromisoformat(raw.replace(" ", "T", 1))
    except ValueError:
        return raw
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.isoformat()
