"""Module-level cache observer hook.

The cache driver lives in the framework and must not import application
metric singletons. To still get hit/miss/error telemetry, each
application registers a callback at boot time via
:func:`set_cache_observer` and the driver invokes it after every
operation through :func:`notify_cache_event`.

The callback signature is intentionally narrow:

    callback(operation: str, outcome: str, key: str, size_bytes: int | None)

- ``operation``: "get" | "put" | "forget" | "add"
- ``outcome``: "hit" | "miss" | "set" | "deleted" | "noop" | "error"
- ``key``: the cache key WITHOUT the driver prefix
- ``size_bytes``: serialised payload size when known, else ``None``

Callbacks must be cheap (sub-millisecond) and never raise — the driver
swallows exceptions so a broken observer cannot break the cache.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

_logger = logging.getLogger("cara.cache.Observer")

CacheObserver = Callable[[str, str, str, int | None], None]

_OBSERVER: CacheObserver | None = None


_KNOWN_SCOPES = frozenset({
    "product", "products", "category", "search", "trending",
    "brand", "home", "deals", "sitemap", "admin", "analytics",
    "experiment", "lock", "stampede", "idempotency", "health", "budget",
})


def scope_for_cache_key(key: str) -> str:
    """Bucket a raw cache key into a low-cardinality ``scope`` label."""
    if not key:
        return "generic"
    head = key.split(":", 1)[0]
    if head in _KNOWN_SCOPES:
        return head
    if head.startswith("verify"):
        return "verify"
    return "generic"


def set_cache_observer(observer: CacheObserver | None) -> None:
    """Register (or clear with ``None``) the process-wide observer."""
    global _OBSERVER
    _OBSERVER = observer


def notify_cache_event(
    operation: str,
    outcome: str,
    key: str,
    size_bytes: int | None = None,
) -> None:
    """Best-effort notification — never raises into the driver."""
    cb = _OBSERVER
    if cb is None:
        return
    try:
        cb(operation, outcome, key, size_bytes)
    except Exception:
        _logger.warning("cache observer callback failed", exc_info=True)
        return
