"""A claimed cooldown is only KEPT by a run that spends it.

``wrap_with_idempotency`` gates on the app's lifecycle hook, which for poll
jobs claims the per-source cooldown (SETNX + TTL) BEFORE the callback runs.
Until this pin the key survived a raising callback: the queue's retries,
seconds later, met the live cooldown, returned ``None`` from the gate and
were settled as successes — the retry budget did nothing and the source went
unpolled until the window expired. The claim must be handed back on failure
(and on a lease throttle, the same "no work happened" shape), kept on
success, and released only while it is still OURS.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from cara.exceptions import IdempotencyOverlapException
from cara.queues.idempotency import MakesIdempotentBase
from cara.testing.fakes.CacheFake import CacheFake

# The package barrel re-exports the CLASS under the submodule's name; reach
# the module object through sys.modules so monkeypatching hits its globals.
#
# TWO MODULES, because the concern spans two: the cooldown claim and its
# release live in `ClaimsSourceCooldown` (which resolves `Cache` and `config`),
# and the orchestrator that hands the claim back on the failure path lives in
# `MakesIdempotentBase`. A rebind has to land in the module that reads the name.
module = sys.modules["cara.queues.idempotency.ClaimsSourceCooldown"]
orchestrator = sys.modules["cara.queues.idempotency.MakesIdempotentBase"]

COOLDOWN_KEY = "collection_cooldown:orders:31"


@pytest.fixture
def cache(monkeypatch):
    fake = CacheFake()
    # ONE fake, BOTH namespaces. The claim reads `Cache` in the cooldown mixin
    # and the lock/result/fence read it in the orchestrator; patching only one
    # leaves the other reaching for the real facade, which raises before a
    # container is booted.
    monkeypatch.setattr(module, "Cache", fake)
    monkeypatch.setattr(orchestrator, "Cache", fake)
    # `config` is the cooldown's alone — the orchestrator stopped importing it
    # when the claim moved out, so there is nothing to rebind there.
    monkeypatch.setattr(module, "config", lambda key, default=None: default)
    return fake


class _PullOrders(MakesIdempotentBase):
    """The app-side shape: the lifecycle hook IS the cooldown claim."""

    source_cooldown_minutes = {"orders": 5}
    cooldown_grain_attrs = ("channel_id",)
    cooldown_requires_grains = True
    # A recurring poll: the cooldown, not the result cache, is the gate.
    idempotency_cache_results = False

    def __init__(self, channel_id: int = 31) -> None:
        super().__init__()
        self.source = "orders"
        self.channel_id = channel_id

    def should_execute_based_on_lifecycle(self) -> bool:
        return self._claim_source_cooldown()


def _run(job: _PullOrders, callback):
    return asyncio.run(job.wrap_with_idempotency(callback))


def test_a_raising_callback_hands_the_cooldown_back_so_the_retry_runs(cache):
    polls: list[str] = []

    async def failing():
        polls.append("first")
        raise ConnectionError("marketplace 503")

    with pytest.raises(ConnectionError):
        _run(_PullOrders(), failing)

    assert not cache.has(COOLDOWN_KEY), "the cooldown outlived the failed run"

    async def retry():
        polls.append("retry")
        return "pulled"

    assert _run(_PullOrders(), retry) == "pulled"
    assert polls == ["first", "retry"]


def test_a_successful_run_keeps_the_cooldown(cache):
    polls: list[str] = []

    async def poll():
        polls.append("poll")
        return "pulled"

    assert _run(_PullOrders(), poll) == "pulled"
    assert cache.has(COOLDOWN_KEY)
    assert cache.ttl_of(COOLDOWN_KEY) == 5 * 60

    # The window is still live: the duplicate dispatch is the skip it was
    # always meant to be.
    assert _run(_PullOrders(), poll) is None
    assert polls == ["poll"]


def test_the_release_is_owner_fenced(cache):
    """A window that lapsed and was re-claimed by a later poll is not ours."""

    async def lapse_then_fail():
        cache.forget(COOLDOWN_KEY)
        assert cache.add(COOLDOWN_KEY, "later-poll", 300) is True
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        _run(_PullOrders(), lapse_then_fail)

    assert cache.get(COOLDOWN_KEY) == "later-poll", (
        "the failed run deleted a claim it no longer owned"
    )


def test_a_lease_throttle_hands_the_cooldown_back_with_the_envelope(cache, monkeypatch):
    """``retry_on_idempotency_overlap`` redelivers the SAME envelope once the
    owner releases its lease; without the release that redelivery would meet
    its own cooldown and be settled as done without ever running."""
    monkeypatch.setattr(_PullOrders, "retry_on_idempotency_overlap", True)
    owner = _PullOrders()
    owner._idempotency_key = owner.generate_idempotency_key()
    assert owner.acquire_job_lock() is True

    async def never():
        raise AssertionError("the throttled copy must not run")

    with pytest.raises(IdempotencyOverlapException):
        _run(_PullOrders(), never)

    assert not cache.has(COOLDOWN_KEY)


def test_a_run_that_never_claimed_releases_nothing(cache):
    """A second dispatch inside the window is refused by the gate; it holds
    no claim, so its exit path must not touch the first run's window."""
    cache.add(COOLDOWN_KEY, "first-run", 300)

    async def never():
        raise AssertionError("the gate must refuse this run")

    assert _run(_PullOrders(), never) is None
    assert cache.get(COOLDOWN_KEY) == "first-run"
