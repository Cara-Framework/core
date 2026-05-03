"""Timebox — constant-time wrapper for security-sensitive operations.

Laravel's ``Illuminate\\Support\\Timebox`` parity. Pads the
execution of ``callback`` so it always takes at least
``microseconds`` total — defeats timing oracles that infer
internal branches (cache hit vs miss, user-found vs not-found,
hash compare success vs failure) from response duration::

    user = Timebox().call(
        lambda tb: User.where_email(email).first(),
        microseconds=200_000,  # 200 ms minimum
    )

After the callback returns, ``Timebox`` busy-waits (or sleeps,
depending on slack) until the elapsed time meets the floor.
Always sleeping the *full* floor regardless of success matches
Laravel — partial padding leaks the same signal we're trying to
mask.

If the callback raises, the exception propagates *after* the
padding completes, so error-vs-success paths take the same wall
time too.
"""

from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")


class Timebox:
    """Run a callback so its total wall-time is at least ``microseconds``."""

    def call(
        self,
        callback: Callable[["Timebox"], T],
        microseconds: int = 0,
    ) -> T:
        """Run ``callback(self)`` and pad to ``microseconds`` minimum.

        ``callback`` receives the timebox instance so it can call
        :meth:`return_early` (Laravel parity) for branches that
        intentionally opt out of padding (e.g. genuine errors that
        should fail fast in development).
        """
        if microseconds < 0:
            raise ValueError("microseconds must be non-negative")

        self._return_early = False
        target_seconds = microseconds / 1_000_000.0
        start = time.perf_counter()

        exc: BaseException | None = None
        result: T | None = None
        try:
            result = callback(self)
        except BaseException as e:  # noqa: BLE001 — propagate after padding
            exc = e

        if not self._return_early:
            self._sleep_until(start, target_seconds)

        if exc is not None:
            raise exc
        return result  # type: ignore[return-value]

    def return_early(self) -> None:
        """Skip the timing pad for this call — Laravel parity."""
        self._return_early = True

    @staticmethod
    def _sleep_until(start: float, target_seconds: float) -> None:
        # ``time.sleep`` resolution is OS-dependent (typically ~1ms on
        # Linux/macOS) — fine for the multi-hundred-ms floors that
        # auth flows use, where the measurement noise dominates.
        remaining = target_seconds - (time.perf_counter() - start)
        if remaining > 0:
            time.sleep(remaining)


__all__ = ["Timebox"]
