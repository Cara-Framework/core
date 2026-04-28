"""Defer — schedule callbacks to run after the current scope exits.

Laravel 11's ``defer()`` helper parity. Lets you queue cleanup or
fire-and-forget work that should run *after* the response is sent
(or after the current request/job lifecycle completes), without
adding a queue job for trivially-quick tasks::

    def list_products(request):
        defer(lambda: log_search(request.query))   # runs after return
        return paginated_view(...)

The runtime hook is provided by :func:`flush` — call it from the
response middleware (``after_response`` hook) or from a CLI's
``finally`` block. Outside a request lifecycle, ``Defer.scope()``
context-manages a flush so ad-hoc CLI scripts still work::

    with Defer.scope():
        do_work()
        defer(cleanup_temp_files)
    # cleanup runs here, after ``do_work`` returns.

Errors inside deferred callbacks are caught and logged (never
raised) — by definition the caller has already returned, so
re-raising would surface to a different (or no) request entirely.
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Callable, List

logger = logging.getLogger(__name__)

# Per-thread queue. Web frameworks running each request on its own
# thread / asyncio task get isolation for free; if the same loop
# fires multiple deferred-using contexts in sequence, each
# ``flush()`` clears them.
_state = threading.local()


def _queue() -> List[Callable[[], None]]:
    if not hasattr(_state, "queue"):
        _state.queue = []
    return _state.queue


class Defer:
    """Static facade for the deferred-callback queue."""

    @staticmethod
    def push(callback: Callable[[], None]) -> None:
        """Add ``callback`` to the deferred queue — also exposed as :func:`defer`."""
        _queue().append(callback)

    @staticmethod
    def flush() -> int:
        """Run every queued callback in registration order.

        Returns the number of callbacks executed. Exceptions from
        individual callbacks are logged at ``ERROR`` and swallowed so
        one bad cleanup doesn't poison the rest of the queue.
        """
        queue = _queue()
        if not queue:
            return 0
        # Take a snapshot + reset before running so callbacks that
        # themselves call ``defer`` register against the next flush
        # (matches Laravel's "deferred-of-deferred" semantics).
        pending = list(queue)
        queue.clear()
        ran = 0
        for callback in pending:
            try:
                callback()
                ran += 1
            except Exception as e:  # noqa: BLE001
                logger.error("Deferred callback failed: %s", e, exc_info=True)
        return ran

    @staticmethod
    def pending() -> int:
        """Number of callbacks currently waiting to run."""
        return len(_queue())

    @staticmethod
    def clear() -> None:
        """Drop the queue without running anything (test cleanup)."""
        _queue().clear()

    @staticmethod
    @contextmanager
    def scope():
        """Context manager that flushes on exit — for CLI scripts.

        On normal exit *and* on exception, queued callbacks run.
        Exceptions from the wrapped block re-raise after flushing so
        callers see the original error (not a swallowed defer failure).
        """
        try:
            yield
        finally:
            Defer.flush()


def defer(callback: Callable[[], None]) -> None:
    """Functional shorthand for :meth:`Defer.push` — Laravel ``defer()``."""
    Defer.push(callback)


__all__ = ["Defer", "defer"]
