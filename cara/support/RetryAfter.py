"""Reading an upstream's ``Retry-After`` answer (RFC 9110 §10.2.3).

One parser for every outbound caller — the framework's own ``PendingRequest``
and every product connector that honours a throttle's floor. The header comes
in two forms: delay-seconds (``"120"``) or an HTTP-date
(``"Wed, 21 Oct 2026 07:28:00 GMT"``). Copies of this parser drifted: one read
only ``int()`` and dropped every HTTP-date to "no wait", one used
``str.isdigit`` and so took ``"²"`` and fullwidth digits (then ``float``
raised), one read only the exact-case ``Retry-After`` key of a plain dict.

The rules here:

- the header is found whatever its case, in any mapping or ``(name, value)``
  pair list (``httpx.Headers``, ``requests``' case-insensitive dict, a dict);
- delay-seconds are ASCII digits, with an optional ASCII fraction (some
  upstreams send ``"1.5"``; reading it beats discarding it);
- an HTTP-date is turned into the seconds left until it, against ``now``
  (UTC; a date without a zone is read as UTC), and a date already past is 0;
- anything else — absent, blank, negative, garbled — is ``None``: the upstream
  named no readable wait, and the caller falls back to its own schedule;
- ``cap`` bounds the answer, so a hostile or broken upstream cannot park a
  worker for days.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

_DELAY_SECONDS = re.compile(r"\d+(?:\.\d+)?", re.ASCII)


def _header(headers: Any, name: str) -> str | None:
    """One header's value whatever its case; None when absent or unreadable."""
    if headers is None:
        return None
    wanted = name.lower()
    try:
        pairs = headers.items() if isinstance(headers, Mapping) else headers
        for key, value in pairs:
            if str(key).lower() == wanted:
                return None if value is None else str(value)
    except TypeError, ValueError, AttributeError:
        return None
    return None


def retry_after_seconds(
    headers: Any,
    *,
    cap: float | None = None,
    now: datetime | None = None,
) -> float | None:
    """The upstream's ``Retry-After`` in seconds, or None when it named none.

    ``headers`` is any header mapping or pair list; ``cap`` (when given) is
    the longest wait honoured; ``now`` pins the clock an HTTP-date is read
    against (tests). The answer is never negative.
    """
    raw = (_header(headers, "Retry-After") or "").strip()
    if not raw:
        return None
    if _DELAY_SECONDS.fullmatch(raw):
        seconds = float(raw)
    else:
        try:
            when = parsedate_to_datetime(raw)
        except TypeError, ValueError, IndexError:
            return None
        if when is None:
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = max(0.0, (when - (now or datetime.now(UTC))).total_seconds())
    if cap is not None:
        seconds = min(seconds, max(0.0, float(cap)))
    return seconds


__all__ = ["retry_after_seconds"]
