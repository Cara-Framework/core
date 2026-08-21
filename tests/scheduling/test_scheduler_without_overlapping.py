"""Scheduler-level ``without_overlapping`` — flag, mutex and release.

Three separate seams have to hold or the protection is a no-op:

1. ``ScheduleBuilder.without_overlapping()`` must land the flag in
   ``options``, the dict the driver actually receives — stashing it on
   the builder instance only made this a silent no-op for years.
2. It must be set BEFORE the terminal ``.interval()`` / ``.daily()`` /
   ``.cron()`` call, because that call is what dispatches ``options``
   to the driver. A flag set afterwards reaches nothing.
3. ``APSchedulerDriver`` must wrap the callback in the cache mutex —
   acquired with a unique owner token and released via
   compare-and-delete on BOTH the success and the exception path.
   A run that crashes without releasing wedges the slot until the TTL
   lapses; a release without the owner fence deletes a PEER's freshly
   acquired lock and lets a third copy start.

APScheduler's own ``max_instances`` only guards same-process re-entry,
so this cache mutex is the only thing standing between a multi-process
scheduler deployment and every pod firing the same entry at the same
wall-clock minute.

No real scheduler is started: the wrapper and the registration are
cara's code, the engine is not, and cara declares no apscheduler
dependency (see ``tests/scheduling/test_scheduler_trigger_parsing.py``).
"""

from __future__ import annotations

import asyncio
import importlib
from typing import Any

import pytest

from cara.scheduling.drivers.APSchedulerDriver import APSchedulerDriver
from cara.scheduling.ScheduleBuilder import ScheduleBuilder
from cara.testing.fakes.CacheFake import CacheFake

# The MODULE, not the class the ``drivers`` barrel exports under the same
# name — the mutex reads its ``Cache`` / ``Log`` bindings as module
# globals, so the fake has to be installed there.
driver_module = importlib.import_module("cara.scheduling.drivers.APSchedulerDriver")
_wrap_without_overlapping = driver_module._wrap_without_overlapping


class _SilentLog:
    """Swallows the driver's skip/registration logging.

    The ``Log`` facade resolves through the application container, which
    a framework unit test does not boot. Replacing the module binding
    keeps the assertion about locking rather than about logging.
    """

    @staticmethod
    def info(*_args, **_kwargs) -> None:
        """Discard an info line."""
        return None

    @staticmethod
    def error(*_args, **_kwargs) -> None:
        """Discard an error line."""
        return None

    @staticmethod
    def warning(*_args, **_kwargs) -> None:
        """Discard a warning line."""
        return None


@pytest.fixture
def cache(monkeypatch) -> CacheFake:
    """Serve the driver module's ``Cache`` facade from an in-memory fake.

    ``CacheFake.add`` is put-if-absent and ``forget_if`` is
    compare-and-delete — the same two primitives the Redis driver
    provides atomically, so the owner fence is exercised for real.
    """
    fake = CacheFake()
    monkeypatch.setattr(driver_module, "_Cache", fake)
    monkeypatch.setattr(driver_module, "Log", _SilentLog)
    return fake


class _RecordingDriver:
    """Captures whatever a terminal trigger call hands to the driver."""

    def __init__(self) -> None:
        """Start with an empty call log."""
        self.calls: list[dict[str, Any]] = []

    def schedule_job(
        self,
        identifier: str,
        callback: Any,
        schedule_spec: dict[str, Any],
        options: dict[str, Any],
    ) -> None:
        """Record one registration, snapshotting ``options`` by value."""
        self.calls.append(
            {
                "identifier": identifier,
                "callback": callback,
                "schedule_spec": dict(schedule_spec),
                "options": dict(options or {}),
            }
        )


class _EngineFreeDriver(APSchedulerDriver):
    """The real driver with its engine and trigger construction stubbed.

    ``schedule_job`` — including the instrumentation and mutex wrapping
    that is the subject here — runs unmodified.
    """

    def _create_scheduler(self):
        """Return an inert stand-in for the APScheduler engine."""

        class _Inert:
            running = False

            @staticmethod
            def remove_job(**_kwargs) -> None:
                return None

        return _Inert()

    def _build_trigger(self, spec):
        """Skip APScheduler entirely — the spec stands in for the trigger."""
        return spec


# ── ScheduleBuilder: the flag has to reach the driver's options ───────


class TestBuilderPropagatesTheFlag:
    """``options`` is the only channel the driver reads."""

    def test_flag_and_timeout_land_in_driver_options(self) -> None:
        """Both keys, on the terminal call, with the operator's timeout."""
        driver = _RecordingDriver()
        builder = ScheduleBuilder(driver, "send:digest", lambda: None, options={})

        builder.without_overlapping(timeout=900).daily(hour=2)

        assert driver.calls, "the terminal .daily() never called schedule_job"
        options = driver.calls[0]["options"]
        assert options.get("without_overlapping") is True
        assert options.get("lock_timeout") == 900

    def test_absent_flag_leaves_the_option_unset(self) -> None:
        """Default-off: enabling it implicitly would change every entry."""
        driver = _RecordingDriver()

        ScheduleBuilder(driver, "send:digest", lambda: None, options={}).daily(hour=2)

        assert driver.calls[0]["options"].get("without_overlapping") in (None, False)

    def test_flag_set_after_the_terminal_call_reaches_nothing(self) -> None:
        """The terminal trigger call dispatches ``options`` immediately.

        Pinned because the fluent API reads as if order did not matter:
        ``.daily(hour=2).without_overlapping()`` returns ``self`` and
        looks configured, but the registration already happened with
        the unprotected options dict.
        """
        driver = _RecordingDriver()
        builder = ScheduleBuilder(driver, "send:digest", lambda: None, options={})

        builder.daily(hour=2).without_overlapping(timeout=900)

        assert len(driver.calls) == 1
        assert driver.calls[0]["options"].get("without_overlapping") in (None, False)


# ── Driver: the option decides whether the callback gets wrapped ──────


class TestDriverWrapsOnlyWhenAsked:
    """The registered callable is what the engine will fire."""

    def _registered_callback(self, driver: _EngineFreeDriver) -> Any:
        """The single callable in the driver's registry."""
        assert len(driver._job_registry) == 1
        return driver._job_registry[0][1]

    def test_flagged_job_is_wrapped_in_the_mutex(self, cache) -> None:
        """A held lock must stop the registered callable from running."""
        ran: list[str] = []
        driver = _EngineFreeDriver()

        driver.schedule_job(
            "sweep:inbox",
            lambda: ran.append("hit"),
            {"type": "interval", "seconds": 30},
            {"silent": True, "without_overlapping": True, "lock_timeout": 60},
        )
        cache.add("scheduler:lock:sweep:inbox", "peer-owner", 60)

        assert self._registered_callback(driver)() is None
        assert ran == []

    def test_unflagged_job_is_not_wrapped(self, cache) -> None:
        """No flag, no lock — an unrelated key must not gate the run."""
        ran: list[str] = []
        driver = _EngineFreeDriver()

        driver.schedule_job(
            "sweep:inbox",
            lambda: ran.append("hit"),
            {"type": "interval", "seconds": 30},
            {"silent": True},
        )
        cache.add("scheduler:lock:sweep:inbox", "peer-owner", 60)

        self._registered_callback(driver)()
        assert ran == ["hit"]

    def test_lock_timeout_defaults_to_one_day(self, cache) -> None:
        """A flagged entry with no explicit timeout must still cap the
        TTL, so a holder that dies mid-run cannot wedge the slot
        forever. 86400 mirrors the documented driver fallback."""
        observed: list[int | None] = []
        driver = _EngineFreeDriver()

        driver.schedule_job(
            "sweep:inbox",
            lambda: observed.append(cache.ttl_of("scheduler:lock:sweep:inbox")),
            {"type": "interval", "seconds": 30},
            {"silent": True, "without_overlapping": True},
        )
        self._registered_callback(driver)()

        assert observed == [86400]


# ── The mutex wrapper itself ──────────────────────────────────────────


class TestSyncWrapper:
    """Synchronous callbacks — the common scheduled-job shape."""

    def test_a_free_slot_runs_and_releases(self, cache) -> None:
        """Releasing on success is what lets the NEXT tick run at all."""
        ran: list[str] = []

        def _callback() -> str:
            ran.append("hit")
            return "ok"

        wrapped = _wrap_without_overlapping("job:free", _callback, lock_timeout=60)

        assert wrapped() == "ok"
        assert ran == ["hit"]
        assert not cache.has("scheduler:lock:job:free")

    def test_a_held_slot_skips_without_running(self, cache) -> None:
        """Laravel semantics: the contended run is skipped, not queued."""
        ran: list[str] = []
        cache.add("scheduler:lock:job:held", "peer-owner", 60)

        wrapped = _wrap_without_overlapping(
            "job:held", lambda: ran.append("hit"), lock_timeout=60
        )

        assert wrapped() is None
        assert ran == []
        assert cache.get("scheduler:lock:job:held") == "peer-owner"

    def test_the_lock_ttl_is_the_configured_timeout(self, cache) -> None:
        """The TTL caps tail damage when a holder dies mid-run."""
        observed: list[int | None] = []

        wrapped = _wrap_without_overlapping(
            "job:ttl",
            lambda: observed.append(cache.ttl_of("scheduler:lock:job:ttl")),
            lock_timeout=900,
        )
        wrapped()

        assert observed == [900]

    def test_a_crashed_run_still_releases(self, cache) -> None:
        """Otherwise one exception silences the schedule until the TTL
        lapses — up to a full day on the default timeout."""

        def _boom() -> None:
            raise RuntimeError("kaboom")

        wrapped = _wrap_without_overlapping("job:crashy", _boom, lock_timeout=60)

        with pytest.raises(RuntimeError):
            wrapped()
        assert not cache.has("scheduler:lock:job:crashy")

    def test_release_is_owner_fenced(self, cache) -> None:
        """The finishing run must not delete a peer's lock.

        Scenario: our TTL lapsed mid-run, a peer re-acquired with its
        own token, and only then did we finish. A bare ``forget`` would
        drop the peer's lock and let a THIRD copy start while two were
        already running; ``forget_if`` compares the owner first.
        """
        key = "scheduler:lock:job:overrun"

        def _overrunning() -> str:
            cache.forget(key)
            assert cache.add(key, "peer-owner", 60) is True
            return "did-work"

        wrapped = _wrap_without_overlapping(
            "job:overrun", _overrunning, lock_timeout=60
        )

        assert wrapped() == "did-work"
        assert cache.get(key) == "peer-owner"

    def test_each_fire_uses_a_distinct_owner_token(self, cache) -> None:
        """A shared constant owner would make the fence decorative."""
        seen: list[Any] = []

        wrapped = _wrap_without_overlapping(
            "job:owner",
            lambda: seen.append(cache.get("scheduler:lock:job:owner")),
            lock_timeout=60,
        )
        wrapped()
        wrapped()

        assert len(seen) == 2
        assert seen[0] != seen[1]


class TestAsyncWrapper:
    """Coroutine callbacks take a separate branch — same contract."""

    def test_a_free_slot_runs_and_releases(self, cache) -> None:
        """The async branch must await the callback, not return the coroutine."""
        ran: list[str] = []

        async def _callback() -> str:
            await asyncio.sleep(0)
            ran.append("hit")
            return "async-ok"

        wrapped = _wrap_without_overlapping("job:async", _callback, lock_timeout=60)

        assert asyncio.run(wrapped()) == "async-ok"
        assert ran == ["hit"]
        assert not cache.has("scheduler:lock:job:async")

    def test_a_held_slot_skips_without_running(self, cache) -> None:
        """Skipping must still be awaitable — the caller awaits either way."""
        ran: list[str] = []

        async def _callback() -> str:
            ran.append("hit")
            return "async-ok"

        cache.add("scheduler:lock:job:async-held", "peer-owner", 60)
        wrapped = _wrap_without_overlapping(
            "job:async-held", _callback, lock_timeout=60
        )

        assert asyncio.run(wrapped()) is None
        assert ran == []

    def test_a_crashed_run_still_releases(self, cache) -> None:
        """Same finally-branch guarantee as the sync wrapper."""

        async def _boom() -> None:
            raise RuntimeError("kaboom")

        wrapped = _wrap_without_overlapping(
            "job:async-crashy", _boom, lock_timeout=60
        )

        with pytest.raises(RuntimeError):
            asyncio.run(wrapped())
        assert not cache.has("scheduler:lock:job:async-crashy")
