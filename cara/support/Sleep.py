"""Sleep — testable sleep facade.

Laravel 10's ``Illuminate\\Support\\Sleep`` parity. A drop-in for
``time.sleep`` / ``asyncio.sleep`` that records every call when
``fake()`` is active, so tests can assert wait behaviour without
actually sleeping::

    # production
    Sleep.for_(250).milliseconds()
    await Sleep.for_(2).seconds().async_sleep()

    # test
    Sleep.fake()
    do_work()
    Sleep.assert_slept_times(3)
    Sleep.assert_slept_for(seconds=2.75)
    Sleep.fake(False)

Implements:

* ``Sleep.for_(n).seconds()`` / ``.milliseconds()`` / ``.microseconds()``
  / ``.minutes()`` — terminal blocking sleeps.
* ``.async_sleep()`` — terminal awaitable sleep.
* Fake mode with sequence assertions and total-elapsed assertions.

All durations normalise to seconds internally so test assertions
can compare regardless of which builder method was used.
"""

from __future__ import annotations

import asyncio
import time
from typing import ClassVar, List, Optional


class Sleep:
    """Fluent, testable wrapper around blocking + async sleeps."""

    __slots__ = ("_seconds",)

    # When True, sleeps are recorded into ``_recorded`` instead of
    # actually invoking ``time.sleep`` / ``asyncio.sleep``.
    _faking: ClassVar[bool] = False
    _recorded: ClassVar[List[float]] = []

    def __init__(self, seconds: float) -> None:
        self._seconds = float(seconds)

    # ── Builders ────────────────────────────────────────────────────

    @classmethod
    def for_(cls, duration: float) -> "Sleep":
        """Start a fluent sleep — call a unit method to terminate."""
        return cls(duration)

    def seconds(self) -> None:
        self._dispatch(self._seconds)

    def milliseconds(self) -> None:
        self._dispatch(self._seconds / 1000.0)

    def microseconds(self) -> None:
        self._dispatch(self._seconds / 1_000_000.0)

    def minutes(self) -> None:
        self._dispatch(self._seconds * 60.0)

    async def async_sleep(self) -> None:
        """Awaitable variant — uses ``asyncio.sleep`` (or fake recorder)."""
        if Sleep._faking:
            Sleep._recorded.append(self._seconds)
            return
        await asyncio.sleep(self._seconds)

    # ── Dispatch ───────────────────────────────────────────────────

    @staticmethod
    def _dispatch(seconds: float) -> None:
        if Sleep._faking:
            Sleep._recorded.append(seconds)
            return
        time.sleep(seconds)

    # ── Test helpers ───────────────────────────────────────────────

    @classmethod
    def fake(cls, value: bool = True) -> None:
        """Enable / disable recording mode. Resets the recorded log."""
        cls._faking = value
        cls._recorded = []

    @classmethod
    def assert_slept_times(cls, expected: int) -> None:
        actual = len(cls._recorded)
        if actual != expected:
            raise AssertionError(
                f"Expected {expected} sleeps, got {actual}: {cls._recorded}"
            )

    @classmethod
    def assert_slept_for(cls, *, seconds: Optional[float] = None) -> None:
        """Assert total recorded sleep time matches ``seconds`` (with epsilon)."""
        if seconds is None:
            return
        total = sum(cls._recorded)
        if abs(total - seconds) > 1e-9:
            raise AssertionError(
                f"Expected total sleep {seconds}s, got {total}s: {cls._recorded}"
            )

    @classmethod
    def assert_never_slept(cls) -> None:
        if cls._recorded:
            raise AssertionError(f"Expected no sleeps, got: {cls._recorded}")

    @classmethod
    def recorded(cls) -> List[float]:
        """Return a copy of recorded sleep durations (in seconds)."""
        return list(cls._recorded)


__all__ = ["Sleep"]
