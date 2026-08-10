"""Behaviour pins for the per-source poll cooldown claim.

The mechanism is one atomic ``Cache.add`` (SETNX + TTL) per
``(source, grains)`` tuple. What matters is that the claim is atomic (a
check-then-act double-fires under two scheduler ticks), that the key shape is
stable (in-flight Redis keys must survive a deploy), and that the escape
hatches fail OPEN — a cooldown is a politeness bound on an upstream, never the
authority on whether work may happen.
"""

from __future__ import annotations

import sys

import pytest

from cara.queues.idempotency import MakesIdempotentBase

# ``import cara.queues.idempotency.MakesIdempotentBase as module`` binds the
# CLASS: the package barrel re-exported that name over the submodule. Reach
# the module object through sys.modules so monkeypatching hits its globals.
module = sys.modules["cara.queues.idempotency.MakesIdempotentBase"]


@pytest.fixture(autouse=True)
def _cache(monkeypatch):
    """A real SETNX: first writer wins, later writers are refused."""
    store: dict[str, tuple[str, int]] = {}

    class _Cache:
        @staticmethod
        def add(key, value, ttl):
            if key in store:
                return False
            store[key] = (value, ttl)
            return True

    monkeypatch.setattr(module, "Cache", _Cache)
    monkeypatch.setattr(module, "config", lambda key, default=None: default)
    return store


class _Poll(MakesIdempotentBase):
    source_cooldown_minutes = {"reports": 5, "inventory": 10}
    cooldown_grain_attrs = ("workspace_id", "connection_id", "value")
    cooldown_requires_grains = True

    def __init__(self, **attrs):
        super().__init__()
        for name, value in attrs.items():
            setattr(self, name, value)


class _GlobalPoll(MakesIdempotentBase):
    source_cooldown_minutes = {"trending": 5, "url": 60}
    cooldown_grain_attrs = ("region", "value")
    cooldown_requires_grains = False

    def __init__(self, **attrs):
        super().__init__()
        for name, value in attrs.items():
            setattr(self, name, value)


# ── The claim ────────────────────────────────────────────────────────


def test_first_caller_wins_and_the_second_is_blocked(_cache):
    first = _Poll(source="reports", workspace_id=1)
    second = _Poll(source="reports", workspace_id=1)

    assert first._claim_source_cooldown() is True
    assert second._claim_source_cooldown() is False


def test_a_different_grain_is_a_different_poll(_cache):
    assert _Poll(source="reports", workspace_id=1)._claim_source_cooldown() is True
    assert _Poll(source="reports", workspace_id=2)._claim_source_cooldown() is True


def test_the_ttl_is_the_source_specific_window(_cache):
    _Poll(source="inventory", workspace_id=1)._claim_source_cooldown()

    (_value, ttl) = next(iter(_cache.values()))
    assert ttl == 10 * 60


def test_an_unmapped_source_uses_the_default_window(_cache):
    _Poll(source="whatever", workspace_id=1)._claim_source_cooldown()

    (_value, ttl) = next(iter(_cache.values()))
    assert ttl == MakesIdempotentBase.default_source_cooldown_minutes * 60


def test_config_can_retune_the_default_window(monkeypatch, _cache):
    monkeypatch.setattr(
        module,
        "config",
        lambda key, default=None: (
            30 if key == "jobs.source_cooldown_minutes" else default
        ),
    )

    _Poll(source="whatever", workspace_id=1)._claim_source_cooldown()

    (_value, ttl) = next(iter(_cache.values()))
    assert ttl == 30 * 60


# ── Key shape ────────────────────────────────────────────────────────


def test_the_key_is_the_prefix_plus_source_plus_truthy_grains_in_order(_cache):
    _GlobalPoll(source="keyword", region="uk", value="airpods")._claim_source_cooldown()

    assert list(_cache) == ["collection_cooldown:keyword:uk:airpods"]


def test_absent_grains_collapse_out_of_the_key(_cache):
    """The key must stay byte-identical to what a partially-attributed job
    produced before, or a deploy invalidates every in-flight cooldown."""
    _GlobalPoll(source="keyword", value="airpods")._claim_source_cooldown()

    assert list(_cache) == ["collection_cooldown:keyword:airpods"]


def test_a_bare_source_claims_a_global_key_when_grains_are_optional(_cache):
    _GlobalPoll(source="trending")._claim_source_cooldown()

    assert list(_cache) == ["collection_cooldown:trending"]


# ── Escape hatches, all fail open ────────────────────────────────────


def test_a_job_without_a_source_is_never_throttled(_cache):
    class _NotAPoll(MakesIdempotentBase):
        pass

    assert _NotAPoll()._claim_source_cooldown() is True
    assert _cache == {}


def test_the_force_flag_bypasses_the_cooldown(_cache):
    assert _Poll(source="reports", workspace_id=1)._claim_source_cooldown() is True

    forced = _Poll(source="reports", workspace_id=1, force=True)
    assert forced._claim_source_cooldown() is True


def test_a_falsy_force_flag_does_not_bypass(_cache):
    assert _Poll(source="reports", workspace_id=1)._claim_source_cooldown() is True
    blocked = _Poll(source="reports", workspace_id=1, force=False)
    assert blocked._claim_source_cooldown() is False


def test_requiring_grains_fails_open_rather_than_claiming_a_fleet_wide_key(_cache):
    """A source-only key would throttle every entity at once."""
    assert _Poll(source="reports")._claim_source_cooldown() is True
    assert _cache == {}


# ── The hook stays a pass-through by default ─────────────────────────


def test_the_default_hook_does_not_claim_anything(_cache):
    """Inheriting the mixin must not turn any job carrying a ``source``
    attribute into a cooldown participant."""

    class _Incidental(MakesIdempotentBase):
        source = "reports"

    assert _Incidental().should_collect_again() is True
    assert _cache == {}
