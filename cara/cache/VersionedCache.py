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
the increment calls. Callers that need to swallow Redis errors and degrade to an
"unversioned" 0 stamp keep their own try/except around ``read()`` /
``bump()`` — the primitive itself is a thin, faithful wrapper over the
facade and lets exceptions propagate.
"""

from __future__ import annotations

from collections.abc import Callable

from cara.facades import Cache


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
