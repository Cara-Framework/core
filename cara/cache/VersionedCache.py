"""Version-key cache stamp primitive.

Cache invalidation across this codebase uses the *version-key* pattern:
reads embed a monotonically-incrementing integer stamp into the cache
key, and writes invalidate everything downstream by bumping that single
stamp — old keys orphan and expire on their own TTL. This avoids the
cartesian-product ``Cache.forget`` loops that can never enumerate every
filter dimension a reader might add.

Two operations recur verbatim at every version-stamped site an app
grows (brand/category caches, per-user notification/wishlist/history
stamps, ...):

* **read** — ``Cache.increment(key, 0, ttl)``. Counter keys are written
  by Redis ``INCRBY`` under the driver's dedicated counter namespace.
  ``increment(key, 0, ttl)`` is the canonical "read counter" idiom: it
  preserves the atomic counter representation and materialises a missing
  key deterministically as 0 (with the TTL written on first touch so a
  later bump can't orphan entries by resetting the TTL).

* **bump** — ``Cache.increment(key, 1, ttl)``. Atomic Redis ``INCRBY``,
  so two concurrent mutations can't lose a bump the way a ``Cache.get``
  + ``Cache.put`` read-modify-write could (a TOCTOU race that left
  stale aggregates through the next TTL window).

``VersionedCache`` folds those two ops behind one object so every
version-stamped site shares one implementation instead of re-spelling
the increment calls. ``read()`` / ``bump()`` are thin, faithful wrappers
over the facade and let exceptions propagate.

A read cache under a stamp must NEVER degrade to a fallback stamp when the
stamp is unreadable. Every bump moves the stamp to >= 1, so an entry kept
under a substitute ``0`` is never invalidated: during a partial cache fault
(read fails, write works) the reader would be served a ``v0`` page that is
arbitrarily stale. ``read_or_none()`` answers ``None`` instead, and
``remember()`` turns that into "compute uncached" — the source of truth is
the callback, so the read cache is disposable (``strict=False``) and a cache
outage costs a cold read, never a 500. ``try_bump()`` is the writer's
best-effort counterpart: a failed bump is logged, never raised into the
write that already committed.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, TypeVar

from cara.facades import Cache

_logger = logging.getLogger("cara.cache")

_T = TypeVar("_T")


class VersionedCache:
    """A single monotonic version stamp behind one cache key.

    ``ttl`` may be a plain int or a zero-arg callable resolving one at
    call time (so per-deploy ``config(...)`` overrides stay live without
    re-reading config on construction).
    """

    __slots__ = ("_key", "_ttl")

    def __init__(self, key: str, ttl: int | Callable[[], int]) -> None:
        self._key = key
        self._ttl = ttl

    def _resolve_ttl(self) -> int:
        ttl = self._ttl
        return int(ttl() if callable(ttl) else ttl)

    def read(self) -> int:
        """Current stamp. Materialises a missing key as 0.

        ``Cache.increment(key, 0, ttl)`` — never ``Cache.get`` (values and
        counters intentionally occupy separate Redis namespaces).
        """
        return int(Cache.increment(self._key, 0, self._resolve_ttl()))

    def bump(self) -> int:
        """Atomically advance the stamp by one (Redis ``INCRBY``)."""
        return int(Cache.increment(self._key, 1, self._resolve_ttl()))

    def read_or_none(self) -> int | None:
        """Current stamp, or ``None`` when the cache cannot answer.

        Never a substitute number: a guessed stamp names a key no bump
        will ever retire.
        """
        try:
            return self.read()
        except Exception as exc:
            _logger.warning(
                "version stamp read failed for %s: %s", self._key, type(exc).__name__
            )
            return None

    def try_bump(self) -> int | None:
        """Advance the stamp; log and answer ``None`` on a cache fault."""
        try:
            return self.bump()
        except Exception as exc:
            _logger.warning(
                "version stamp bump failed for %s: %s", self._key, type(exc).__name__
            )
            return None

    def remember(
        self,
        key_for: Callable[[int], str],
        ttl: int,
        callback: Callable[[], _T],
    ) -> _T | Any:
        """Read-through cache under the current stamp.

        ``key_for(stamp)`` spells the entry's key. An unreadable stamp
        computes uncached; the entry itself is disposable (``strict=False``),
        so a cache outage never fails the read.
        """
        stamp = self.read_or_none()
        if stamp is None:
            return callback()
        return Cache.remember(key_for(stamp), ttl, callback, strict=False)
