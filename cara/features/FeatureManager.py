"""Feature flags — cached, fail-open runtime gate (Laravel Pennant-lite).

The cache wiring, negative caching, deterministic percentage bucketing,
fail-open resolution and flush are framework concerns; only where flag
state comes from differs per app. Apps plug that in once at boot::

    from cara.facades import Feature

    Feature.resolve_using(read_my_flag_row)  # (key) -> dict | ABSENT

    if Feature.active("new-checkout", identifier=str(user.id)):
        ...

Resolver contract: return :data:`ABSENT` for a missing flag, or a small
plain-dict snapshot — ``{"value": bool}`` plus an optional
``{"percentage": 0..100}`` for cohort rollouts. Snapshots are cached
(via the ``Cache`` facade) so hot paths touch the backing store at most
once per TTL; both hits and misses are cached.

Semantics:

* **Fail-open to ``default``.** A missing flag, an unregistered
  resolver, a cache outage or a resolver error all resolve to the
  caller's ``default``. The only thing that flips a path on is an
  explicit, successfully-read enabled flag.
* **Explicit boolean wins over percentage** — a disabled flag is off
  for everyone; an enabled flag with a percentage buckets by
  ``identifier`` (no identifier → >0% means on).
* **Deterministic buckets** — the same identifier is always in or out
  for a given key; growing the percentage only ADDS users.

Testing::

    with Feature.fake({"new-checkout": True, "ramp": 30}):
        ...
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any

# Sentinel stored when a flag is absent, so the miss is negative-cached
# instead of re-querying the backing store every call. A plain string on
# purpose: it must survive a cache round-trip (JSON/pickle) and still
# compare equal.
ABSENT = "__absent__"

_DEFAULT_CACHE_PREFIX = "feature_flag:"
_DEFAULT_CACHE_TTL = 120  # seconds — flush() is the fast path, TTL the backstop


def bucket(key: str, identifier: str) -> int:
    """Deterministically bucket ``identifier`` into 0..99 for flag ``key``.

    Stable across processes and runs (SHA-256, not the salted built-in
    ``hash``) — a user inside the rollout stays inside as the percentage
    grows, and only flips out if it shrinks below their bucket.
    """
    digest = hashlib.sha256(f"{key}:{identifier}".encode()).hexdigest()
    return int(digest, 16) % 100


class _FakeScope:
    """Context manager returned by :meth:`FeatureManager.fake`."""

    def __init__(self, manager: FeatureManager):
        self._manager = manager

    def __enter__(self) -> _FakeScope:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self._manager.restore()


class FeatureManager:
    """Cached, fail-open feature-flag gate with a pluggable resolver."""

    def __init__(self) -> None:
        self._resolver: Callable[[str], Any] | None = None
        self._cache_prefix = _DEFAULT_CACHE_PREFIX
        self._cache_ttl = _DEFAULT_CACHE_TTL
        self._fake_flags: dict[str, Any] | None = None

    # ── wiring ────────────────────────────────────────────────────────

    def resolve_using(
        self,
        resolver: Callable[[str], Any],
        *,
        cache_prefix: str | None = None,
        cache_ttl: int | None = None,
    ) -> None:
        """Register the app's flag reader: ``(key) -> dict | ABSENT``.

        The dict snapshot carries ``{"value": bool}`` and optionally
        ``{"percentage": 0..100}``. Call once at boot.
        """
        self._resolver = resolver
        if cache_prefix is not None:
            self._cache_prefix = cache_prefix
        if cache_ttl is not None:
            self._cache_ttl = cache_ttl

    @staticmethod
    def from_config(config_key: str = "features") -> Callable[[str], Any]:
        """Ready-made resolver reading flags from app configuration.

        Zero-table adoption path::

            Feature.resolve_using(FeatureManager.from_config("features"))

        with a ``config/features.py`` exposing plain values::

            NEW_CHECKOUT = True
            RAMP = {"value": True, "percentage": 30}

        Config values: ``bool`` (plain flag), ``int`` (enabled at that
        rollout percentage) or a full ``{"value", "percentage"}`` dict.
        Missing keys resolve to :data:`ABSENT` (→ the caller's default).
        """

        def _resolver(key: str) -> Any:
            from cara.configuration import config

            _missing = object()
            raw = config(f"{config_key}.{key}", _missing)
            if raw is _missing or raw is None:
                return ABSENT
            if isinstance(raw, bool):
                return {"value": raw}
            if isinstance(raw, int):
                return {"value": True, "percentage": max(0, min(100, raw))}
            if isinstance(raw, dict):
                return {
                    "value": bool(raw.get("value", False)),
                    **(
                        {"percentage": max(0, min(100, int(raw["percentage"])))}
                        if raw.get("percentage") is not None
                        else {}
                    ),
                }
            return ABSENT

        return _resolver

    # ── resolution ────────────────────────────────────────────────────

    def active(
        self,
        key: str,
        default: bool = False,
        *,
        identifier: str | None = None,
    ) -> bool:
        """Resolve flag ``key`` to a boolean. Never raises.

        ``default`` is the fail-open value — returned when the flag is
        absent, no resolver is registered, or any read errors.
        ``identifier`` (user id, session, tenant) buckets percentage
        rollouts deterministically; ignored for plain boolean flags.
        """
        if self._fake_flags is not None:
            state = self._fake_flags.get(key, ABSENT)
            if isinstance(state, bool):
                state = {"value": state}
            elif isinstance(state, int):
                state = {"value": True, "percentage": max(0, min(100, state))}
        else:
            if self._resolver is None:
                return default
            try:
                state = self._resolve_state(key)
            except Exception:
                # Cache down, store down, resolver bug — fail OPEN to the
                # caller's default. A flag-layer outage must never take a
                # hot path down with it.
                self._warn("feature flag resolution failed; using default", key)
                return default

        if state == ABSENT:
            return default

        enabled = bool(state.get("value", False))
        percentage = state.get("percentage")

        # Explicit boolean wins: a disabled flag is off for everyone, and
        # a flag with no percentage is a plain global boolean.
        if percentage is None or not enabled:
            return enabled

        # Percentage rollout. Without an identifier there is nothing to
        # bucket — the percentage degrades to a global boolean.
        if identifier is None:
            return percentage > 0

        return bucket(key, identifier) < percentage

    def _resolve_state(self, key: str) -> Any:
        """Cached read-through: cache → resolver, caching hits AND misses."""
        from cara.facades import Cache

        cache_key = f"{self._cache_prefix}{key}"

        _missing = object()
        cached = Cache.get(cache_key, _missing)
        if cached is not _missing:
            return cached

        state = self._resolver(key)
        Cache.put(cache_key, state, self._cache_ttl)
        return state

    # ── invalidation ──────────────────────────────────────────────────

    def flush(self, key: str | None = None) -> None:
        """Bust the cached resolution for ``key`` (or every flag).

        Call after an admin edit so the new value is visible immediately;
        the TTL is the backstop. Fail-safe — a cache error is swallowed.
        """
        from cara.facades import Cache

        try:
            if key is None:
                Cache.forget_by_prefix(self._cache_prefix)
            else:
                Cache.forget(f"{self._cache_prefix}{key}")
        except Exception:
            self._warn("feature flag cache flush failed", key)

    # ── testing ───────────────────────────────────────────────────────

    def fake(self, flags: dict[str, Any] | None = None) -> _FakeScope:
        """Pin flag states in memory, bypassing cache and resolver.

        Values: ``bool`` (plain flag), ``int`` (enabled at that rollout
        percentage) or a full state dict. Unlisted keys resolve to the
        caller's ``default`` — flags defaulting off is normal, not an
        error. Usable as a context manager for auto-restore.
        """
        self._fake_flags = dict(flags) if flags else {}
        return _FakeScope(self)

    def restore(self) -> None:
        """Drop the fake — resolution goes back through cache + resolver."""
        self._fake_flags = None

    # ── internals ─────────────────────────────────────────────────────

    @staticmethod
    def _warn(message: str, key: str | None) -> None:
        # Lazy facade import: this module is imported by cara.facades
        # during its own initialisation — a module-top Log import would
        # re-enter the half-built package (same pattern as HttpClient).
        try:
            from cara.facades import Log

            Log.warning(message, extra={"feature_flag": key})
        except Exception:
            pass


# Single shared instance — imported as ``from cara.facades import Feature``.
Feature = FeatureManager()
