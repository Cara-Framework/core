"""How an outbound webhook reads a receiver's non-2xx answer.

Every non-2xx is retried on the sender's backoff except ``410 Gone`` — the
receiver saying "stop". Market norm (Stripe, Shopify): a 401 while the
receiver still holds a rotated-out secret, a 404 during its deploy, a
408/425/429 or a 409 from its lock all land on their own once the receiver
is right, inside the attempt budget. Treating every 4xx as permanent drops
the event silently during exactly those windows. A sender that re-reads the
endpoint's CURRENT secret on each attempt therefore loses nothing to a
secret rotation.

An answer that names a wait (``Retry-After``, seconds or an HTTP-date) is
honoured: the next attempt comes no sooner than the wait, capped at
:data:`WEBHOOK_RETRY_AFTER_CEILING_SECONDS` so a hostile or broken receiver
cannot park a delivery for days. Parsing is :func:`retry_after_seconds`'s.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from .RetryAfter import retry_after_seconds

#: The only answers that end a delivery without a retry.
TERMINAL_WEBHOOK_STATUSES = frozenset({410})
#: The longest wait a receiver's ``Retry-After`` may impose on one attempt.
WEBHOOK_RETRY_AFTER_CEILING_SECONDS = 3600


def webhook_answer_is_terminal(status: int) -> bool:
    """True when the receiver's non-2xx answer ends the delivery."""
    return int(status) in TERMINAL_WEBHOOK_STATUSES


def webhook_retry_after(headers: Any, *, now: datetime | None = None) -> int | None:
    """The receiver's ``Retry-After`` as whole seconds within the ceiling.

    ``None`` when the answer names no readable wait.
    """
    seconds = retry_after_seconds(
        headers, cap=WEBHOOK_RETRY_AFTER_CEILING_SECONDS, now=now
    )
    return None if seconds is None else int(math.ceil(seconds))


__all__ = [
    "TERMINAL_WEBHOOK_STATUSES",
    "WEBHOOK_RETRY_AFTER_CEILING_SECONDS",
    "webhook_answer_is_terminal",
    "webhook_retry_after",
]
