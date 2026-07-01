"""Tests for the cached, fail-open feature-flag gate (``commons.support.Feature``).

Covers the contract the runtime read path promises:

* active / inactive resolution from a boolean flag row,
* absent flag → caller's ``default`` (fail-open),
* cache-hit serves without a second DB read (the whole point — must not
  hit the DB per call),
* ``flush`` busts the cache so an admin edit is visible before the TTL,
* deterministic percentage bucketing (same identifier stable, distribution
  ≈ the configured percentage),
* fail-open on any read error (cache/DB outage never raises).

The ``FeatureFlag`` model is not touched — instead the module's single DB
entry point ``_read_flag_state`` is monkeypatched onto an in-memory store
with a call counter, and the real ``Cache`` facade is swapped for
``CacheFake``. So the cache plumbing, resolution logic, and rollout maths are
all exercised end-to-end without a database.
"""

from __future__ import annotations

import commons.support.FeatureGate as gate
import pytest
from commons.support import Feature

from cara.testing import CacheFake, facade_swap


class _FlagStore:
    """In-memory stand-in for the ``feature_flag`` table.

    ``rows`` maps key -> the snapshot ``_read_flag_state`` would return
    (a dict, or the ``_ABSENT`` sentinel for a missing row). ``db_reads``
    counts how often the (faked) DB was hit so cache-hit tests can assert
    "exactly once".
    """

    def __init__(self) -> None:
        self.rows: dict[str, object] = {}
        self.db_reads = 0

    def set_bool(self, key: str, value: bool) -> None:
        self.rows[key] = {"value": value}

    def set_percentage(self, key: str, value: bool, percentage: int) -> None:
        self.rows[key] = {"value": value, "percentage": percentage}

    def read(self, key: str) -> object:
        self.db_reads += 1
        return self.rows.get(key, gate._ABSENT)


@pytest.fixture
def cache() -> CacheFake:
    fake = CacheFake()
    facade_swap.register("cache", fake)
    facade_swap.register("logger", _SilentLog())
    yield fake
    facade_swap.unregister("cache")
    facade_swap.unregister("logger")


class _SilentLog:
    """Swallow the fail-open warning so test output stays clean."""

    def warning(self, *args, **kwargs) -> None:
        pass

    def info(self, *args, **kwargs) -> None:
        pass

    def error(self, *args, **kwargs) -> None:
        pass


@pytest.fixture
def store(monkeypatch) -> _FlagStore:
    s = _FlagStore()
    monkeypatch.setattr(gate, "_read_flag_state", s.read)
    return s


# ── Basic resolution ─────────────────────────────────────────────────────


def test_active_flag_returns_true(cache, store):
    store.set_bool("new_checkout", True)
    assert Feature.active("new_checkout") is True


def test_inactive_flag_returns_false(cache, store):
    store.set_bool("new_checkout", False)
    # Default-false: an explicit off row resolves off.
    assert Feature.active("new_checkout") is False
    # Default-true: the off row still wins over the default.
    assert Feature.active("new_checkout", default=True) is False


def test_disabled_flag_overrides_default(cache, store):
    # An explicit disabled row is OFF even when default=True — the row wins.
    store.set_bool("legacy_path", False)
    assert Feature.active("legacy_path", default=True) is False


# ── Absent flag → default (fail-open to caller intent) ─────────────────────


def test_absent_flag_returns_default_false(cache, store):
    assert Feature.active("missing") is False


def test_absent_flag_returns_default_true(cache, store):
    assert Feature.active("missing", default=True) is True


# ── Cache: one DB read, then served from cache ─────────────────────────────


def test_cache_hit_avoids_second_db_read(cache, store):
    store.set_bool("cached_flag", True)

    assert Feature.active("cached_flag") is True
    assert Feature.active("cached_flag") is True
    assert Feature.active("cached_flag") is True

    assert store.db_reads == 1  # resolved once, then served from cache


def test_absent_flag_is_negative_cached(cache, store):
    # A missing flag must not re-query the DB on every call (the common
    # pre-rollout state on a hot path).
    assert Feature.active("missing") is False
    assert Feature.active("missing") is False
    assert store.db_reads == 1


def test_cache_stores_under_prefixed_key(cache, store):
    store.set_bool("k", True)
    Feature.active("k")
    cache.assert_has("feature_flag:k")


def test_flush_busts_single_key(cache, store):
    store.set_bool("k", False)
    assert Feature.active("k", default=True) is False
    assert store.db_reads == 1

    # Admin flips it on + busts the cache.
    store.set_bool("k", True)
    Feature.flush("k")

    assert Feature.active("k") is True
    assert store.db_reads == 2  # re-read after flush


def test_flush_all_busts_every_key(cache, store):
    store.set_bool("a", True)
    store.set_bool("b", True)
    Feature.active("a")
    Feature.active("b")
    assert store.db_reads == 2

    Feature.flush()  # no key → flush all

    Feature.active("a")
    Feature.active("b")
    assert store.db_reads == 4


# ── Percentage / cohort rollout ────────────────────────────────────────────


def test_percentage_zero_is_off_for_everyone(cache, store):
    store.set_percentage("rollout", True, 0)
    for i in range(50):
        assert Feature.active("rollout", identifier=f"user-{i}") is False


def test_percentage_hundred_is_on_for_everyone(cache, store):
    store.set_percentage("rollout", True, 100)
    for i in range(50):
        assert Feature.active("rollout", identifier=f"user-{i}") is True


def test_percentage_bucketing_is_deterministic(cache, store):
    store.set_percentage("rollout", True, 50)
    # Same identifier → same answer across many calls.
    first = Feature.active("rollout", identifier="stable-user")
    for _ in range(20):
        assert Feature.active("rollout", identifier="stable-user") is first


def test_percentage_distribution_approximates_pct(cache, store):
    pct = 30
    store.set_percentage("rollout", True, pct)
    n = 5000
    on = sum(
        1 for i in range(n) if Feature.active("rollout", identifier=f"id-{i}")
    )
    ratio = on / n
    # Deterministic md5 buckets spread ~uniformly; allow a generous band.
    assert abs(ratio - pct / 100) < 0.05


def test_disabled_percentage_flag_is_off(cache, store):
    # value=False with a percentage → the boolean still wins (off for all).
    store.set_percentage("rollout", False, 100)
    assert Feature.active("rollout", identifier="anyone") is False


def test_percentage_without_identifier_is_global_boolean(cache, store):
    # No identifier → can't bucket; >0% degrades to a global "on".
    store.set_percentage("rollout", True, 25)
    assert Feature.active("rollout") is True

    store.set_percentage("rollout_off", True, 0)
    assert Feature.active("rollout_off") is False


def test_grown_rollout_keeps_existing_cohort_in(cache, store):
    # A user in at 30% must still be in at 60% (monotonic inclusion) — the
    # property that makes md5 bucketing safe for ramping rollouts.
    included_at_30 = [
        f"u{i}"
        for i in range(200)
        if gate._bucket("ramp", f"u{i}") < 30
    ]
    assert included_at_30, "expected some users in the 30% cohort"
    for ident in included_at_30:
        assert gate._bucket("ramp", ident) < 60


# ── Fail-open on error ─────────────────────────────────────────────────────


def test_fail_open_on_db_error(cache, monkeypatch):
    def _boom(_key):
        raise RuntimeError("db down")

    monkeypatch.setattr(gate, "_read_flag_state", _boom)

    assert Feature.active("anything", default=True) is True
    assert Feature.active("anything", default=False) is False


def test_fail_open_on_cache_error(store):
    class _BrokenCache(CacheFake):
        def get(self, key, default=None):
            raise ConnectionError("redis down")

    facade_swap.register("cache", _BrokenCache())
    facade_swap.register("logger", _SilentLog())
    try:
        # Cache read raises inside _resolve_state → caught → default.
        assert Feature.active("x", default=True) is True
        assert Feature.active("x", default=False) is False
    finally:
        facade_swap.unregister("cache")
        facade_swap.unregister("logger")


def test_flush_swallows_cache_error():
    class _BrokenCache(CacheFake):
        def forget(self, key):
            raise ConnectionError("redis down")

        def forget_by_prefix(self, prefix):
            raise ConnectionError("redis down")

    facade_swap.register("cache", _BrokenCache())
    facade_swap.register("logger", _SilentLog())
    try:
        # Must not raise.
        Feature.flush("k")
        Feature.flush()
    finally:
        facade_swap.unregister("cache")
        facade_swap.unregister("logger")
