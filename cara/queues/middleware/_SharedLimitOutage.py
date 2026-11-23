"""Once-per-outage latch for the job limits' shared store."""

from __future__ import annotations

import contextlib
import threading

from cara.facades import Log


class _SharedLimitOutage:
    """Tell an operator once that a job limit fell back to this process, and once
    that it is shared again.

    ``RateLimited`` (the shared rate authority) and ``ThrottlesExceptions`` (a
    shared cache counter and gate) keep their state in the cache and fall back to their in-process bucket for any call the store
    cannot answer. That call happens once per JOB, so a warning per call would
    bury the log under one line for every job that ran during the outage; the
    latch turns the run of failures into one line when it starts and one when
    it ends.
    """

    def __init__(self, limit_name: str) -> None:
        self._limit_name = limit_name
        self._down = False
        self._lock = threading.Lock()

    def failed(self, key: str, exc: BaseException) -> None:
        with self._lock:
            first = not self._down
            self._down = True
        if first:
            self._log(
                "%s %s: the shared rate store cannot answer (%s: %s); every worker "
                "process enforces the limit on its own until it recovers",
                self._limit_name,
                key,
                exc.__class__.__name__,
                exc,
            )

    def answered(self) -> None:
        with self._lock:
            recovered = self._down
            self._down = False
        if recovered:
            self._log(
                "%s: the shared rate store answers again; the limit is shared",
                self._limit_name,
            )

    @staticmethod
    def _log(message: str, *args: object) -> None:
        # Logging must not turn a limit decision into a job failure.
        with contextlib.suppress(ImportError, RuntimeError):
            Log.warning(message, *args, category="cara.queue.middleware")
