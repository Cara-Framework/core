"""Regression pins for ``cara.features`` — the framework feature-flag gate.

Pins the pluggable resolver, read-through caching of hits and misses,
fail-open resolution, explicit boolean over percentage, deterministic
bucketing, and the test fake.
"""

from __future__ import annotations

import pytest

from cara.features import ABSENT, FeatureManager, bucket
from cara.testing import CacheFake, facade_swap


class _SilentLog:
    def warning(self, *args, **kwargs):
        pass


@pytest.fixture
def cache():
    fake = CacheFake()
    facade_swap.register("cache", fake)
    facade_swap.register("logger", _SilentLog())
    yield fake
    facade_swap.unregister("cache")
    facade_swap.unregister("logger")


class _Store:
    def __init__(self, rows=None):
        self.rows = rows or {}
        self.reads = 0

    def read(self, key):
        self.reads += 1
        return self.rows.get(key, ABSENT)


def _manager(store: _Store) -> FeatureManager:
    manager = FeatureManager()
    manager.resolve_using(store.read, cache_prefix="test_feature:")
    return manager


class TestResolution:
    def test_enabled_flag_is_active(self, cache):
        manager = _manager(_Store({"x": {"value": True}}))
        assert manager.active("x") is True

    def test_absent_flag_falls_back_to_default(self, cache):
        manager = _manager(_Store())
        assert manager.active("missing") is False
        assert manager.active("missing", default=True) is True

    def test_no_resolver_registered_is_fail_open(self, cache):
        manager = FeatureManager()
        assert manager.active("anything") is False
        assert manager.active("anything", default=True) is True

    def test_resolver_error_is_fail_open(self, cache):
        manager = FeatureManager()
        manager.resolve_using(lambda key: 1 / 0)
        assert manager.active("x", default=True) is True

    def test_reads_are_cached_including_misses(self, cache):
        store = _Store({"x": {"value": True}})
        manager = _manager(store)

        for _ in range(5):
            manager.active("x")
            manager.active("missing")

        assert store.reads == 2  # one per key, rest served from cache

    def test_flush_busts_a_single_key(self, cache):
        store = _Store({"x": {"value": True}})
        manager = _manager(store)

        manager.active("x")
        manager.flush("x")
        manager.active("x")

        assert store.reads == 2


class TestPercentageRollout:
    def test_disabled_flag_beats_percentage(self, cache):
        manager = _manager(_Store({"x": {"value": False, "percentage": 100}}))
        assert manager.active("x", identifier="u1") is False

    def test_identifier_bucketing_is_deterministic(self, cache):
        manager = _manager(_Store({"ramp": {"value": True, "percentage": 30}}))

        first = manager.active("ramp", identifier="user-42")
        assert all(
            manager.active("ramp", identifier="user-42") == first for _ in range(5)
        )
        assert first == (bucket("ramp", "user-42") < 30)

    def test_growing_percentage_keeps_existing_cohort(self, cache):
        inside = [f"u{i}" for i in range(200) if bucket("ramp", f"u{i}") < 30]
        # Every identifier inside 30% stays inside at 60%.
        assert all(bucket("ramp", ident) < 60 for ident in inside)

    def test_no_identifier_degrades_to_global_boolean(self, cache):
        manager = _manager(_Store({"x": {"value": True, "percentage": 1}}))
        assert manager.active("x") is True

        manager2 = _manager(_Store({"y": {"value": True, "percentage": 0}}))
        assert manager2.active("y") is False


class TestFake:
    def test_fake_pins_flags_and_restores(self, cache):
        store = _Store({"x": {"value": False}})
        manager = _manager(store)

        with manager.fake({"x": True, "ramp": 100}):
            assert manager.active("x") is True
            assert manager.active("ramp", identifier="u1") is True
            assert manager.active("unlisted", default=True) is True
            assert store.reads == 0  # fake bypasses cache + resolver

        assert manager.active("x") is False  # back to the real store


class TestConfigResolver:
    def test_reads_flags_from_config(self, cache, monkeypatch):
        values = {
            "features.plain": True,
            "features.ramp": 30,
            "features.rich": {"value": True, "percentage": 100},
        }

        def fake_config(key, default=None):
            return values.get(key, default)

        import cara.configuration

        monkeypatch.setattr(cara.configuration, "config", fake_config)

        manager = FeatureManager()
        manager.resolve_using(
            FeatureManager.from_config("features"), cache_prefix="test_cfg_feature:"
        )

        assert manager.active("plain") is True
        assert manager.active("rich", identifier="u1") is True
        assert manager.active("missing") is False
        assert manager.active("missing", default=True) is True
        assert manager.active("ramp", identifier="u1") == (bucket("ramp", "u1") < 30)
