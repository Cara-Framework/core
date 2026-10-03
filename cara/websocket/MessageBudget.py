"""A per-connection allowance of client frames on an established socket."""

from __future__ import annotations

import time
from collections.abc import Callable

from cara.configuration import config

_DEFAULT_LIMIT = 60
_DEFAULT_WINDOW_SECONDS = 60


class MessageBudget:
    """``limit`` client frames per ``window_seconds``, refilled continuously.

    ``ws.throttle`` bounds how often a client may CONNECT; nothing bounded
    what an established socket may SEND, and a subscribe frame runs an
    authorization query. ``spend()`` answers whether one more frame fits the
    budget; a socket over it is the caller's to close with 4008 (the
    framework's "rate limit exceeded" close code). The bucket starts full, so
    the burst of subscribes a client sends right after connecting passes.
    """

    def __init__(
        self,
        limit: int,
        window_seconds: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("MessageBudget limit must be a positive integer")
        if window_seconds <= 0:
            raise ValueError("MessageBudget window must be positive")
        self._capacity = float(limit)
        self._refill_per_second = float(limit) / float(window_seconds)
        self._tokens = float(limit)
        self._clock = clock
        self._stamp = clock()

    @classmethod
    def configured(cls, name: str = "ws_messages") -> MessageBudget:
        """The budget ``rate.<name>.{limit,window}`` declares (``ws.throttle``'s shape)."""
        try:
            limit = int(config(f"rate.{name}.limit", _DEFAULT_LIMIT))
        except TypeError, ValueError:
            limit = _DEFAULT_LIMIT
        try:
            window = int(config(f"rate.{name}.window", _DEFAULT_WINDOW_SECONDS))
        except TypeError, ValueError:
            window = _DEFAULT_WINDOW_SECONDS
        return cls(max(1, limit), max(1, window))

    def spend(self) -> bool:
        """Take one frame from the budget; ``False`` when none is left."""
        now = self._clock()
        elapsed = max(0.0, now - self._stamp)
        self._stamp = now
        self._tokens = min(
            self._capacity, self._tokens + elapsed * self._refill_per_second
        )
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False


__all__ = ["MessageBudget"]
