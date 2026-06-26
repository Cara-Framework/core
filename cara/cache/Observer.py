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


# Framework-internal cache-key scopes only (Cara cache facade + queue).
# Apps register their own DOMAIN scopes at boot via register_cache_scopes()
# — the same "framework ships the mechanism, app supplies the specifics"
# pattern as set_cache_observer below, so no project vocabulary lives here.
_KNOWN_SCOPES: frozenset[str] = frozenset({
    "lock", "stampede", "idempotency", "health",
})


def register_cache_scopes(*scopes: str) -> None:
    """Add app-specific cache-key scopes for the metrics ``scope`` label.

    Idempotent and additive; call once per process at boot (typically
    alongside :func:`set_cache_observer`).
    """
    global _KNOWN_SCOPES
    _KNOWN_SCOPES = _KNOWN_SCOPES | frozenset(s for s in scopes if s)


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
