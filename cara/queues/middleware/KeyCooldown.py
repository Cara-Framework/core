"""Per-key throttle cooldown: release a cooled-down key's job BEFORE it takes a slot.

An upstream that answers 429 is rationing one key — a seller account, a
retailer, an API credential. The call already waits the throttle out inside
its retry loop (``cara.support.send_with_retry``, up to the per-call wait
budget), but while it waits it holds a worker slot and any concurrency slot
the job took (``ConcurrencyLimited``), and every further job for the same key
does it again: one throttled key can hold half of a fleet's slots.

So the call that ends still throttled opens a short cooldown on its key
(:func:`mark_cooldown`), and :class:`KeyCooldown` — placed BEFORE any
``ConcurrencyLimited`` in ``job.middleware()`` — releases every job for that
key as a :class:`~cara.queues.contracts.JobThrottledException` until it ends:
no attempt spent, no slot held, no in-call sleep. Other keys never see it.

The key is the caller's: a product scopes it by tenant, retailer or
credential. A cooldown is clamped to ``[min_seconds, max_seconds]``; a
throttle that names no ``Retry-After`` gets ``default_seconds``. An
unreachable cache never blocks a job (fail open: the upstream still enforces
its own limit and the in-call wait still applies).
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Callable
from typing import Any

from cara.facades import Cache
from cara.queues.contracts import JobThrottledException
from cara.support import retry_after_seconds

_logger = logging.getLogger("cara.queue.middleware")

DEFAULT_COOLDOWN_SECONDS = 30.0
MIN_COOLDOWN_SECONDS = 5.0
MAX_COOLDOWN_SECONDS = 300.0

_NS = "cara:key-cooldown:"


def _cache():
    """The cache facade (a seam the tests swap)."""
    return Cache


def cooldown_key(key: str) -> str:
    """The cache key a cooldown for ``key`` is stored under."""
    return f"{_NS}{key}"


def mark_cooldown(
    key: str,
    *,
    seconds: float | None = None,
    headers: Any = None,
    default_seconds: float = DEFAULT_COOLDOWN_SECONDS,
    min_seconds: float = MIN_COOLDOWN_SECONDS,
    max_seconds: float = MAX_COOLDOWN_SECONDS,
) -> float:
    """Open (or extend) the cooldown on ``key``; the seconds applied (0 when
    nothing was written).

    ``seconds`` wins; else the throttle's ``Retry-After`` in ``headers``;
    else ``default_seconds``. A cooldown never shortens one already open.
    """
    if not key:
        return 0.0
    wanted = seconds
    if wanted is None and headers is not None:
        wanted = retry_after_seconds(headers, cap=max_seconds)
    cooldown = float(wanted if wanted is not None else default_seconds)
    cooldown = min(max(cooldown, float(min_seconds)), float(max_seconds))
    until = time.time() + cooldown
    stored = cooldown_key(key)
    try:
        cache = _cache()
        current = cache.get(stored)
        if current is None or float(current) < until:
            cache.put(stored, until, int(math.ceil(cooldown)) + 1)
    except Exception as exc:  # noqa: BLE001 — fail open: the upstream still limits
        _logger.warning("key cooldown not recorded for %s: %s", key, type(exc).__name__)
        return 0.0
    return cooldown


def cooldown_remaining(key: str) -> float:
    """Seconds left on ``key``'s cooldown (0 = none, or the cache is down)."""
    if not key:
        return 0.0
    try:
        until = _cache().get(cooldown_key(key))
    except Exception as exc:  # noqa: BLE001 — fail open
        # Fail open (the job runs; the upstream still enforces its limit),
        # but visibly: debug, as this runs on every job of a keyed lane.
        _logger.debug("key cooldown unreadable for %s: %s", key, type(exc).__name__)
        return 0.0
    if until is None:
        return 0.0
    try:
        return max(float(until) - time.time(), 0.0)
    except TypeError, ValueError:
        return 0.0


def clear_cooldown(key: str) -> None:
    """End ``key``'s cooldown now (operators, tests)."""
    try:
        _cache().forget(cooldown_key(key))
    except Exception as exc:  # noqa: BLE001 — the cooldown still expires on its TTL
        # Reported, not swallowed: an operator who cleared a cooldown must
        # learn it is still in force until its TTL ends.
        _logger.warning("key cooldown not cleared for %s: %s", key, type(exc).__name__)


class KeyCooldown:
    """Job middleware: release a cooled-down key's job before it takes a slot.

    ``key`` is a string, or ``None`` for "no key" (the job runs). Place it
    before every ``ConcurrencyLimited`` in ``job.middleware()``.
    """

    def __init__(self, key: str | None) -> None:
        self.key = str(key) if key else None

    async def handle(self, job: Any, next_fn: Callable) -> Any:
        if self.key:
            remaining = cooldown_remaining(self.key)
            if remaining > 0:
                raise JobThrottledException(
                    f"{self.key} is cooling down after a throttle",
                    key=cooldown_key(self.key),
                    retry_after=max(1, math.ceil(remaining)),
                )
        result = next_fn(job)
        if asyncio.iscoroutine(result):
            return await result
        return result


__all__ = [
    "DEFAULT_COOLDOWN_SECONDS",
    "MAX_COOLDOWN_SECONDS",
    "MIN_COOLDOWN_SECONDS",
    "KeyCooldown",
    "clear_cooldown",
    "cooldown_key",
    "cooldown_remaining",
    "mark_cooldown",
]
