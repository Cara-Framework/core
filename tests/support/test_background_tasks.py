"""``schedule_deduped_task``'s self-heal contract, pinned.

The primitive is in-process and loss-tolerant by design (see the module
docstring): a dropped task is only safe because the trigger RE-ARMS on the
next request. Two properties carry that self-heal and nothing guarded them
before — the inflight sentinel is always written WITH a finite TTL, and the
``finally`` block forgets the key on BOTH the success and the failure path.
A regression that dropped the TTL would wedge every caller for the life of
the process, silently and with no failing test.

These run without a live event loop on purpose: with no running loop the
module executes the coroutine synchronously, which makes the ``finally``
observable without racing a background task.
"""

from __future__ import annotations

from typing import Any

import pytest

from cara.concurrency import BackgroundTasks
from cara.concurrency.BackgroundTasks import schedule_deduped_task


class _FakeCache:
    """A cache honouring the ``cara.cache.contracts.Cache`` add/forget door."""

    def __init__(self, *, add_result: bool | Exception = True) -> None:
        self._add_result = add_result
        self.add_calls: list[tuple[str, Any, int | None]] = []
        self.forget_calls: list[str] = []
        self.has_calls: list[str] = []
        self.put_calls: list[tuple[str, Any, int | None]] = []

    def add(self, key: str, value: Any, ttl: int | None = None) -> bool:
        self.add_calls.append((key, value, ttl))
        if isinstance(self._add_result, Exception):
            raise self._add_result
        return self._add_result

    def forget(self, key: str) -> None:
        self.forget_calls.append(key)

    def has(self, key: str) -> bool:
        self.has_calls.append(key)
        return False

    def put(self, key: str, value: Any, ttl: int | None = None) -> None:
        self.put_calls.append((key, value, ttl))


class _AddlessCache(_FakeCache):
    """A driver missing the contract-mandated ``add``."""

    add = None  # type: ignore[assignment]


class _RecordingLog:
    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.debugs: list[str] = []

    def warning(self, message: str, *args: Any, **_kwargs: Any) -> None:
        self.warnings.append(message % args if args else message)

    def debug(self, message: str, *args: Any, **_kwargs: Any) -> None:
        self.debugs.append(message % args if args else message)


@pytest.fixture
def cache(monkeypatch) -> _FakeCache:
    fake = _FakeCache()
    monkeypatch.setattr(BackgroundTasks, "Cache", fake)
    monkeypatch.setattr(BackgroundTasks, "Log", _RecordingLog())
    return fake


async def _succeeds() -> None:
    return None


async def _fails() -> None:
    raise RuntimeError("background boom")


def test_inflight_sentinel_carries_a_finite_ttl(cache):
    """No TTL means a crashed process wedges the dedup key forever."""
    assert schedule_deduped_task(
        dedup_key="ai:summary:42:inflight",
        coro_factory=_succeeds,
        inflight_ttl=7,
    )

    assert cache.add_calls == [("ai:summary:42:inflight", "1", 7)]
    ttl = cache.add_calls[0][2]
    assert isinstance(ttl, int)
    assert ttl > 0


def test_default_inflight_ttl_is_finite(cache):
    schedule_deduped_task(dedup_key="k", coro_factory=_succeeds)

    assert cache.add_calls[0][2] == BackgroundTasks._DEFAULT_INFLIGHT_TTL_SECONDS
    assert BackgroundTasks._DEFAULT_INFLIGHT_TTL_SECONDS > 0


def test_sentinel_is_forgotten_on_success(cache):
    schedule_deduped_task(dedup_key="k", coro_factory=_succeeds)

    assert cache.forget_calls == ["k"]


def test_sentinel_is_forgotten_on_failure(monkeypatch):
    """A crashed task must not poison the key for the whole TTL — the retry
    on the next request IS the self-heal this primitive relies on."""
    fake = _FakeCache()
    log = _RecordingLog()
    monkeypatch.setattr(BackgroundTasks, "Cache", fake)
    monkeypatch.setattr(BackgroundTasks, "Log", log)

    schedule_deduped_task(dedup_key="k", coro_factory=_fails)

    assert fake.forget_calls == ["k"]
    assert any("background task failed" in line for line in log.warnings)


def test_second_caller_bails_while_a_task_is_inflight(monkeypatch):
    fake = _FakeCache(add_result=False)
    monkeypatch.setattr(BackgroundTasks, "Cache", fake)
    monkeypatch.setattr(BackgroundTasks, "Log", _RecordingLog())

    assert schedule_deduped_task(dedup_key="k", coro_factory=_succeeds) is False
    assert fake.forget_calls == []


def test_cache_failure_prefers_running_over_skipping(monkeypatch):
    fake = _FakeCache(add_result=ConnectionError("redis unavailable"))
    monkeypatch.setattr(BackgroundTasks, "Cache", fake)
    monkeypatch.setattr(BackgroundTasks, "Log", _RecordingLog())

    assert schedule_deduped_task(dedup_key="k", coro_factory=_succeeds) is True


def test_add_is_the_only_inflight_path(monkeypatch):
    """Pinned wrong behaviour: a retained ``has`` + ``put`` two-step ran
    whenever ``add`` was absent — a second, racier implementation of the same
    dedup that a driver could select just by shadowing one attribute, with the
    TOCTOU window ``add`` exists to close. ``add`` is mandated by
    ``cara.cache.contracts.Cache``, so there is nothing to fall back to: the
    failure is reported at WARNING and the work runs."""
    fake = _AddlessCache()
    log = _RecordingLog()
    monkeypatch.setattr(BackgroundTasks, "Cache", fake)
    monkeypatch.setattr(BackgroundTasks, "Log", log)

    assert schedule_deduped_task(dedup_key="k", coro_factory=_succeeds) is True

    assert fake.has_calls == []
    assert fake.put_calls == []
    assert any("inflight add failed" in line for line in log.warnings)
