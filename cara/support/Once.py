"""Once — runtime memoization keyed on call site.

Laravel's ``Illuminate\\Support\\Once`` parity. Executes a zero-arg
callable exactly once per unique caller location and caches the
result for the lifetime of the process (or until :meth:`flush` is
called)::

    def expensive_lookup():
        return Once.make(lambda: heavy_computation())

The cache key is derived from the caller's ``(filename, lineno)``
so the same lambda literal at two different sites memoizes
independently — matching Laravel's ``spl_object_hash`` behaviour
on the closure's bound location.

Useful for:

* One-shot configuration lookups inside hot paths.
* Lazy singletons that don't justify a full container binding.
* Test-friendly memoization (``Once.flush()`` resets between cases).
"""

from __future__ import annotations

import sys
import threading
from typing import Any, Callable, Dict, Tuple

# Sentinel distinguishing "cached None" from "not yet computed".
_MISSING = object()


class Once:
    """Process-wide memoization keyed on caller ``(file, line)``."""

    _cache: Dict[Tuple[str, int], Any] = {}
    _lock = threading.Lock()

    @classmethod
    def make(cls, callback: Callable[[], Any]) -> Any:
        """Run ``callback()`` once per unique call site, cache result.

        The first invocation at a given source location runs the
        callback and stores its return value; every subsequent
        invocation at that same location returns the cached value
        without re-executing.

        Thread-safe — concurrent callers race on the lock and only
        one wins the right to compute. Losers see the cached value.
        """
        # Walk one frame back to the caller's site — that's the
        # natural memoization key for a Laravel-style ``once`` call.
        frame = sys._getframe(1)
        key = (frame.f_code.co_filename, frame.f_lineno)

        cached = cls._cache.get(key, _MISSING)
        if cached is not _MISSING:
            return cached

        with cls._lock:
            # Double-check inside the lock — another thread may have
            # populated the cache while we were waiting.
            cached = cls._cache.get(key, _MISSING)
            if cached is not _MISSING:
                return cached
            value = callback()
            cls._cache[key] = value
            return value

    @classmethod
    def flush(cls) -> None:
        """Clear every memoized value — primarily for tests."""
        with cls._lock:
            cls._cache.clear()

    @classmethod
    def has(cls, callback: Callable[[], Any] | None = None) -> bool:  # noqa: ARG003
        """True if the caller's site has a cached value."""
        frame = sys._getframe(1)
        key = (frame.f_code.co_filename, frame.f_lineno)
        return key in cls._cache


def once(callback: Callable[[], Any]) -> Any:
    """Functional shorthand for :meth:`Once.make` — Laravel ``once()`` helper."""
    frame = sys._getframe(1)
    key = (frame.f_code.co_filename, frame.f_lineno)

    cached = Once._cache.get(key, _MISSING)
    if cached is not _MISSING:
        return cached

    with Once._lock:
        cached = Once._cache.get(key, _MISSING)
        if cached is not _MISSING:
            return cached
        value = callback()
        Once._cache[key] = value
        return value


__all__ = ["Once", "once"]
