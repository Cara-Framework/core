"""In-memory fake for the ``Cache`` facade.

Real cache backends (Redis/Memcached) are slow and stateful between
tests. This fake is a plain ``dict`` with optional TTL semantics — the
TTL isn't time-driven (tests shouldn't rely on wall clock); it's
purely tracked so ``forever``/``put(ttl=...)`` round-trip correctly.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional


class CacheFake:
    def __init__(self) -> None:
        self._store: Dict[str, Any] = {}
        self._ttls: Dict[str, Optional[int]] = {}

    # Production-side surface
    def get(self, key: str, default: Any = None) -> Any:
        return self._store.get(key, default)

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        # Contract returns None — Redis/File drivers all return None on
        # put. The fake used to return ``True`` so any test asserting
        # on the return value silently passed against the fake and
        # diverged from production.
        self._store[key] = value
        self._ttls[key] = ttl

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        self.put(key, value, ttl)

    def forever(self, key: str, value: Any) -> None:
        self.put(key, value, None)

    def add(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Put-if-absent. Returns True iff key was newly added.

        Mirrors the real Redis/File driver semantics so atomic-claim
        flows (e.g. cooldown locks) round-trip correctly under tests.
        """
        if key in self._store:
            return False
        self._store[key] = value
        self._ttls[key] = ttl
        return True

    def has(self, key: str) -> bool:
        return key in self._store

    def forget(self, key: str) -> bool:
        existed = key in self._store
        self._store.pop(key, None)
        self._ttls.pop(key, None)
        return existed

    def forget_if(self, key: str, expected_value: Any) -> bool:
        """Atomically delete ``key`` only if it currently equals ``expected_value``.

        Required by the ``Cache`` contract — the primitive
        ``CacheLock.release`` uses to avoid the "A's TTL expires, B
        acquires, A.release deletes B's key" race. The fake used to
        omit this entirely; tests fell back to a non-atomic
        get→forget that hid the race entirely.
        """
        if key in self._store and self._store[key] == expected_value:
            self._store.pop(key, None)
            self._ttls.pop(key, None)
            return True
        return False

    def delete(self, key: str) -> bool:
        return self.forget(key)

    def flush(self) -> None:
        self._store.clear()
        self._ttls.clear()

    def remember(self, key: str, ttl: Optional[int], factory: Callable[[], Any]) -> Any:
        if key in self._store:
            return self._store[key]
        value = factory()
        self.put(key, value, ttl)
        return value

    def increment(self, key: str, amount: int = 1, ttl: Optional[int] = None) -> int:
        """Atomically increment ``key`` by ``amount``.

        Signature now matches the Cache contract — callers passing
        ``ttl=`` (e.g. version-stamp helpers in BrandCache /
        CategoryCache / ConversionCache / BrowsingHistoryService)
        previously crashed against the fake with TypeError. ``ttl``
        applies on first creation only; subsequent increments do not
        refresh the TTL, mirroring Redis ``INCRBY`` semantics.
        """
        existed = key in self._store
        value = int(self._store.get(key, 0)) + amount
        self._store[key] = value
        if not existed:
            self._ttls[key] = ttl
        return value

    def decrement(self, key: str, amount: int = 1, ttl: Optional[int] = None) -> int:
        return self.increment(key, -amount, ttl)

    def forget_pattern(self, pattern: str) -> int:
        """Delete every key matching ``pattern`` (glob-style ``*`` only).

        Mirrors the real driver's contract closely enough for prod
        callers (``HomeCacheInvalidator``, admin cache controller) to
        round-trip under tests. Only ``*`` wildcards are honoured —
        ``?`` / character classes are not used in callers and aren't
        worth modelling here.
        """
        import fnmatch

        keys = [k for k in self._store if fnmatch.fnmatchcase(k, pattern)]
        for k in keys:
            self._store.pop(k, None)
            self._ttls.pop(k, None)
        return len(keys)

    def forget_by_prefix(self, prefix: str) -> int:
        """Delete every key starting with ``prefix``.

        Convenience wrapper over :meth:`forget_pattern` — matches the
        real ``Cache.forget_by_prefix`` so prefix-sweep callers
        (``Cache.forget_by_prefix("category:facets:")``) behave the
        same in production and in tests.
        """
        return self.forget_pattern(f"{prefix}*")

    # ── Test-time helpers ────────────────────────────────────────────

    def all(self) -> Dict[str, Any]:
        return dict(self._store)

    def ttl_of(self, key: str) -> Optional[int]:
        return self._ttls.get(key)

    def assert_has(self, key: str, value: Any = ...) -> None:
        if key not in self._store:
            raise AssertionError(f"Expected cache key {key!r}, not found")
        if value is not ... and self._store[key] != value:
            raise AssertionError(
                f"Cache key {key!r} = {self._store[key]!r}, expected {value!r}"
            )

    def assert_missing(self, key: str) -> None:
        if key in self._store:
            raise AssertionError(f"Expected cache key {key!r} to be missing")

    def clear(self) -> None:
        self.flush()
