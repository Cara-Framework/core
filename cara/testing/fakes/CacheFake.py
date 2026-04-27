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

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        self._store[key] = value
        self._ttls[key] = ttl
        return True

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        return self.put(key, value, ttl)

    def forever(self, key: str, value: Any) -> bool:
        return self.put(key, value, None)

    def has(self, key: str) -> bool:
        return key in self._store

    def forget(self, key: str) -> bool:
        existed = key in self._store
        self._store.pop(key, None)
        self._ttls.pop(key, None)
        return existed

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

    def increment(self, key: str, by: int = 1) -> int:
        value = int(self._store.get(key, 0)) + by
        self._store[key] = value
        return value

    def decrement(self, key: str, by: int = 1) -> int:
        return self.increment(key, -by)

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
