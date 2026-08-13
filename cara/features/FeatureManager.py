"""Feature flags — cached, fail-closed runtime gate (Laravel Pennant-lite).

The cache wiring, negative caching, deterministic percentage bucketing,
fail-closed resolution and strict invalidation are framework concerns; only where flag
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

* **Fail closed.** A missing flag, an unregistered resolver, a cache outage,
  malformed state or a resolver error all resolve to ``False``. The only
  thing that flips a path on is an explicit, successfully-read enabled flag.
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
import logging
from collections.abc import Callable
from typing import Any

# Sentinel stored when a flag is absent, so the miss is negative-cached
# instead of re-querying the backing store every call. A plain string on
# purpose: it must survive a cache round-trip (JSON/pickle) and still
# compare equal.
ABSENT = "__absent__"

_DEFAULT_CACHE_PREFIX = "feature_flag:"
_DEFAULT_CACHE_TTL = 120  # seconds — flush() is the fast path, TTL the backstop
_logger = logging.getLogger("cara.features")


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
    """Cached, fail-closed feature-flag gate with a pluggable resolver."""

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
        if not callable(resolver):
            raise TypeError("Feature resolver must be callable")
        self._resolver = resolver
        if cache_prefix is not None:
            if not isinstance(cache_prefix, str) or not cache_prefix.strip():
                raise ValueError("Feature cache prefix must be a non-empty string")
            self._cache_prefix = cache_prefix
        if cache_ttl is not None:
            if (
                not isinstance(cache_ttl, int)
                or isinstance(cache_ttl, bool)
                or cache_ttl <= 0
            ):
                raise ValueError("Feature cache TTL must be a positive integer")
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
        Missing keys resolve to :data:`ABSENT` (disabled).
        """

        def _resolver(key: str) -> Any:
            from cara.configuration import config  # local: cycle with cara.configuration

            _missing = object()
            raw = config(f"{config_key}.{key}", _missing)
            if raw is _missing or raw is None:
                return ABSENT
            if isinstance(raw, bool):
                return {"value": raw}
            if isinstance(raw, int) and not isinstance(raw, bool):
                if not 0 <= raw <= 100:
                    raise ValueError(
                        f"Feature {key!r} percentage must be between 0 and 100"
                    )
                return {"value": True, "percentage": raw}
            if isinstance(raw, dict):
                return FeatureManager._validate_state(key, raw)
            raise ValueError(
                f"Feature {key!r} must be bool, percentage integer, or state object"
            )

        return _resolver

    # ── resolution ────────────────────────────────────────────────────

    def active(
        self,
        key: str,
        *,
        identifier: str | None = None,
    ) -> bool:
        """Resolve flag ``key`` to a boolean, failing closed.

        ``identifier`` (user id, session, tenant) buckets percentage
        rollouts deterministically; ignored for plain boolean flags.
        """
        if not isinstance(key, str) or not key.strip():
            raise ValueError("Feature key must be a non-empty string")
        if self._fake_flags is not None:
            state = self._fake_flags.get(key, ABSENT)
        else:
            if self._resolver is None:
                return False
            try:
                state = self._resolve_state(key)
            except Exception:
                self._warn("feature flag resolution failed; disabling flag", key)
                return False

        if state == ABSENT:
            return False

        try:
            state = self._validate_state(key, state)
        except TypeError, ValueError:
            self._warn("feature flag state is invalid; disabling flag", key)
            return False

        enabled = bool(state.get("value", False))
        percentage = state.get("percentage")

        # Explicit boolean wins: a disabled flag is off for everyone, and
        # a flag with no percentage is a plain global boolean.
        if percentage is None or not enabled:
            return enabled

        # Percentage rollout requires an identity. Treating a positive
        # percentage as globally enabled when identity propagation breaks
        # would expand a partial rollout to every request.
        if identifier is None:
            return False

        return bucket(key, identifier) < percentage

    def _resolve_state(self, key: str) -> Any:
        """Cached read-through: cache → resolver, caching hits AND misses."""
        from cara.facades import Cache  # local: cycle with cara.facades

        cache_key = f"{self._cache_prefix}{key}"

        _missing = object()
        cached = Cache.get(cache_key, _missing, strict=True)
        if cached is not _missing:
            return cached

        state = self._resolver(key)
        Cache.put(cache_key, state, self._cache_ttl, strict=True)
        return state

    # ── invalidation ──────────────────────────────────────────────────

    def flush(self, key: str | None = None) -> None:
        """Bust the cached resolution for ``key`` (or every flag).

        Call after an admin edit so the new value is visible immediately;
        the TTL is the backstop. A failed invalidation is surfaced because
        silently serving a stale enabled flag would violate fail-closed state.
        """
        from cara.facades import Cache  # local: cycle with cara.facades

        try:
            if key is None:
                Cache.forget_by_prefix(self._cache_prefix)
            else:
                Cache.forget(f"{self._cache_prefix}{key}")
        except Exception as exc:
            self._warn("feature flag cache flush failed", key)
            from cara.exceptions import (  # local: cycle with cara.exceptions
                ServiceUnavailableException,
            )

            raise ServiceUnavailableException(
                "Feature flag cache is temporarily unavailable",
                retry_after=5,
            ) from exc

    # ── testing ───────────────────────────────────────────────────────

    def fake(self, flags: dict[str, Any] | None = None) -> _FakeScope:
        """Pin flag states in memory, bypassing cache and resolver.

        Values: ``bool`` (plain flag), ``int`` (enabled at that rollout
        percentage) or a full state dict. Unlisted keys are disabled.
        Usable as a context manager for auto-restore.
        """
        normalized: dict[str, Any] = {}
        for key, state in (flags or {}).items():
            if isinstance(state, bool):
                state = {"value": state}
            elif isinstance(state, int) and not isinstance(state, bool):
                state = {"value": True, "percentage": state}
            normalized[key] = self._validate_state(key, state)
        self._fake_flags = normalized
        return _FakeScope(self)

    def restore(self) -> None:
        """Drop the fake — resolution goes back through cache + resolver."""
        self._fake_flags = None

    @staticmethod
    def _validate_state(key: str, state: Any) -> dict[str, Any]:
        if not isinstance(state, dict):
            raise ValueError(f"Feature {key!r} state must be an object")
        unknown = set(state) - {"value", "percentage"}
        if unknown:
            raise ValueError(
                f"Feature {key!r} state has unknown keys: "
                f"{', '.join(sorted(str(item) for item in unknown))}"
            )
        if "value" not in state or not isinstance(state["value"], bool):
            raise ValueError(f"Feature {key!r} state requires a boolean value")
        normalized = {"value": state["value"]}
        if "percentage" in state:
            percentage = state["percentage"]
            if (
                not isinstance(percentage, int)
                or isinstance(percentage, bool)
                or not 0 <= percentage <= 100
            ):
                raise ValueError(
                    f"Feature {key!r} percentage must be an integer from 0 to 100"
                )
            normalized["percentage"] = percentage
        return normalized

    # ── internals ─────────────────────────────────────────────────────

    @staticmethod
    def _warn(message: str, key: str | None) -> None:
        # The stdlib logger is bootstrap-safe: importing a facade here would
        # re-enter the half-built ``cara.facades`` package.
        _logger.warning(message, extra={"feature_flag": key})


# Single shared instance — imported as ``from cara.facades import Feature``.
Feature = FeatureManager()

__all__ = ["ABSENT", "Feature", "FeatureManager", "bucket"]

__all__ = ["ABSENT", "Feature", "FeatureManager", "bucket"]
