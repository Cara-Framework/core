"""Strict ISO-8601 moment parsing for client-supplied datetimes.

Product-blind: returns an aware UTC ``pendulum.DateTime`` or raises
``ValueError``; the caller turns that into its own field-keyed 422.

Rejection asserts the RESULT, not just the exception class.
``pendulum.parse`` raises ``ParserError`` (a ``ValueError``) for unparseable
text, but an ISO-8601 duration or interval (``P1D``,
``2026-01-01/2026-02-01``) parses into a ``Duration``/``Interval`` that has no
``in_timezone`` — that must be a client error, not an ``AttributeError`` 500.
A bare date (``2026-05-05``) is read as that day's UTC midnight.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pendulum


def parse_iso_moment(value: Any) -> pendulum.DateTime | None:
    """ISO datetime → aware UTC pendulum; ``None``/blank → ``None``.

    Strict (``pendulum.parse(strict=True)``): a human spelling such as
    ``May 5 2026`` is refused rather than guessed. A ``datetime`` instance (a
    service caller) passes through, a naive one read as UTC. Anything that is
    not a single instant raises ``ValueError``.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, datetime):
        return pendulum.instance(value).in_timezone("UTC")
    if not isinstance(value, str):
        raise ValueError("an ISO datetime must be a string")
    try:
        parsed = pendulum.parse(value.strip(), strict=True)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"not an ISO datetime: {value!r}") from exc
    if not isinstance(parsed, pendulum.DateTime):
        raise ValueError(f"not an ISO datetime: {value!r}")
    return parsed.in_timezone("UTC")


__all__ = ["parse_iso_moment"]
