"""Scheduler registrations that used to misfire silently.

Each of these registered without complaint and then ran wrong: a days-only
or zero interval fired every SECOND (the engine's default for an empty
period); a numeric cron weekday fired a day late (crontab's 0 is Sunday,
the engine's is Monday); an async callable was never run while the fire
counted as a success; one failed lock release wedged its entry for the
whole lock TTL; a misspelled executor dropped the job at its first due
time; and ``run_all`` hid how many tasks failed from ``schedule:work
--once``.

The engine is stubbed as in ``test_scheduler_trigger_parsing.py``: cara
declares no apscheduler dependency, and what is cara's here is the parse,
the forwarding and the wrapping.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import sys
from datetime import UTC, datetime
from types import ModuleType, SimpleNamespace

import pytest

from cara.exceptions import InvalidArgumentException
from cara.scheduling.drivers import _ScheduleTriggers
from cara.scheduling.ScheduleBuilder import ScheduleBuilder
from cara.scheduling.Scheduling import Scheduling
from cara.testing.fakes.CacheFake import CacheFake

driver_module = importlib.import_module("cara.scheduling.drivers.APSchedulerDriver")


class _Trigger:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs


@pytest.fixture(autouse=True)
def _stub_engine(monkeypatch) -> None:
    modules = {
        "apscheduler": ModuleType("apscheduler"),
        "apscheduler.triggers": ModuleType("apscheduler.triggers"),
    }
    for leaf, cls_name in (
        ("cron", "CronTrigger"),
        ("date", "DateTrigger"),
        ("interval", "IntervalTrigger"),
    ):
        module = ModuleType(f"apscheduler.triggers.{leaf}")
        setattr(module, cls_name, type(cls_name, (_Trigger,), {}))
        modules[f"apscheduler.triggers.{leaf}"] = module
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


class _RecordingLog:
    def __init__(self) -> None:
        self.lines: list[tuple[str, str]] = []

    def __getattr__(self, level: str):
        def _record(message, *args, **_kwargs) -> None:
            self.lines.append((level, message % args if args else message))

        return _record


class _Outcomes:
    """Stands in for the Prometheus metrics the driver writes."""

    def __init__(self) -> None:
        self.seen: list[tuple[str, str]] = []
        inert = SimpleNamespace(observe=lambda *_: None, set=lambda *_: None)
        self.scheduled_task_duration_seconds = SimpleNamespace(labels=lambda **_: inert)
        self.scheduled_task_last_run_timestamp_seconds = SimpleNamespace(
            labels=lambda **_: inert
        )
        self.scheduler_last_tick_timestamp_seconds = inert
        self.scheduled_tasks_total = SimpleNamespace(labels=self._labels)

    def _labels(self, task: str, outcome: str):
        return SimpleNamespace(inc=lambda: self.seen.append((task, outcome)))


@pytest.fixture
def log(monkeypatch) -> _RecordingLog:
    recorder = _RecordingLog()
    monkeypatch.setattr(driver_module, "Log", recorder)
    return recorder


@pytest.fixture
def outcomes(monkeypatch) -> _Outcomes:
    recorder = _Outcomes()
    monkeypatch.setattr(driver_module, "MetricsBase", recorder)
    return recorder


class _EngineFreeDriver(driver_module.APSchedulerDriver):
    def _create_scheduler(self):
        return SimpleNamespace(running=False, remove_job=lambda **_kwargs: None)


@pytest.fixture
def driver(log, outcomes) -> _EngineFreeDriver:
    return _EngineFreeDriver(
        {"timezone": "UTC", "executors": {"probes": {"type": "threadpool"}}}
    )


def _register(driver, identifier, callback, spec, **options):
    driver.schedule_job(identifier, callback, spec, {"silent": True, **options})
    return next(cb for jid, cb, _tr, _jo in driver._job_registry if jid == identifier)


# ── 1. Interval period ────────────────────────────────────────────────


class TestIntervalPeriod:
    @pytest.mark.parametrize(
        "period",
        [{"seconds": 0}, {}, {"minutes": -5}, {"hours": 0, "days": 0}],
    )
    def test_a_period_that_is_not_positive_is_refused_with_the_entry_name(
        self, driver, period
    ) -> None:
        with pytest.raises(InvalidArgumentException) as raised:
            driver.schedule_job(
                "flush_ai_enrichment_batch",
                lambda: None,
                {"type": "interval", **period},
                {"silent": True},
            )

        assert "flush_ai_enrichment_batch" in str(raised.value)
        assert driver._job_registry == []

    def test_days_and_weeks_reach_the_trigger_from_the_builder(self, driver) -> None:
        ScheduleBuilder(driver, "weekly_digest", lambda: None, {"silent": True}).interval(
            days=2, weeks=1
        )

        trigger = driver._job_registry[0][2]
        assert trigger.kwargs["days"] == 2
        assert trigger.kwargs["weeks"] == 1


# ── 2. Cron weekdays ──────────────────────────────────────────────────


class TestCronWeekdays:
    @pytest.mark.parametrize(
        ("field", "engine"),
        [
            ("1", "mon"),
            ("0", "sun"),
            ("7", "sun"),
            (3, "wed"),
            ("1-5", "mon,tue,wed,thu,fri"),
            ("5-7", "fri,sat,sun"),
            ("0,6", "sun,sat"),
            ("0,7", "sun"),
            ("*/2", "sun,tue,thu,sat"),
            ("1-5/2", "mon,wed,fri"),
            ("mon", "mon"),
            ("mon-fri", "mon-fri"),
            ("sat,0", "sat,sun"),
            ("*", "*"),
        ],
    )
    def test_numbers_keep_their_crontab_meaning(self, field, engine) -> None:
        assert _ScheduleTriggers._cron_weekdays(field) == engine

    @pytest.mark.parametrize("field", ["8", "5-1", "*/0", "1/x"])
    def test_an_impossible_weekday_is_refused(self, field) -> None:
        with pytest.raises(InvalidArgumentException):
            _ScheduleTriggers._cron_weekdays(field)

    def test_a_cron_expression_and_weekly_are_both_translated(self, driver) -> None:
        cron = driver._build_trigger({"type": "cron", "expression": "0 9 * * 1"})
        weekly = driver._build_trigger({"type": "weekly", "day_of_week": 0, "hour": 9})

        assert cron.kwargs["day_of_week"] == "mon"
        assert weekly.kwargs["day_of_week"] == "sun"

    def test_the_real_engine_fires_on_the_crontab_day(self, monkeypatch) -> None:
        # The real engine where installed: the names must mean the same days.
        for name in ("apscheduler.triggers.cron", "apscheduler.triggers", "apscheduler"):
            monkeypatch.delitem(sys.modules, name)
        cron_module = pytest.importorskip("apscheduler.triggers.cron")
        trigger = cron_module.CronTrigger(
            minute="0",
            hour="9",
            day_of_week=_ScheduleTriggers._cron_weekdays("1-5"),
            timezone="UTC",
        )
        monday = datetime(2026, 11, 23, tzinfo=UTC)
        fires, previous = [], None
        for _ in range(5):
            previous = trigger.get_next_fire_time(previous, previous or monday)
            fires.append(previous.strftime("%a"))

        assert fires == ["Mon", "Tue", "Wed", "Thu", "Fri"]


# ── 3. Async callables ────────────────────────────────────────────────


class TestAsyncCallables:
    def test_the_engine_receives_a_sync_job_that_runs_the_body(
        self, driver, outcomes
    ) -> None:
        ran: list[str] = []

        async def tick() -> str:
            await asyncio.sleep(0)
            ran.append("ran")
            return "done"

        job = _register(driver, "async_tick", tick, {"type": "interval", "seconds": 5})

        assert not inspect.iscoroutinefunction(job)
        assert job() == "done"
        assert ran == ["ran"]
        assert ("async_tick", "success") in outcomes.seen

    def test_a_failing_body_is_a_failure(self, driver, outcomes) -> None:
        async def tick() -> None:
            raise RuntimeError("body failed")

        job = _register(driver, "async_boom", tick, {"type": "interval", "seconds": 5})

        with pytest.raises(RuntimeError):
            job()
        assert ("async_boom", "failure") in outcomes.seen
        assert ("async_boom", "success") not in outcomes.seen

    def test_run_all_runs_the_body(self, driver) -> None:
        ran: list[str] = []

        async def tick() -> None:
            ran.append("ran")

        _register(driver, "async_once", tick, {"type": "interval", "seconds": 5})

        assert driver.run_all() == 1
        assert ran == ["ran"]

    def test_a_sync_wrapper_returning_a_coroutine_runs_it(self, driver) -> None:
        """``Schedule.call`` wraps every callback in a sync function."""
        ran: list[str] = []

        async def tick() -> None:
            ran.append("ran")

        manager = Scheduling(None, "apscheduler")
        manager.add_driver("apscheduler", driver)
        manager.call(tick).interval(seconds=5)

        assert driver.run_all() == 1
        assert ran == ["ran"]


# ── 4. Lock release failure ───────────────────────────────────────────


class _FlakyCache(CacheFake):
    def __init__(self, blips: int) -> None:
        super().__init__()
        self.blips = blips

    def forget_if(self, key, expected_value):
        if self.blips:
            self.blips -= 1
            raise ConnectionError("cache blip during release")
        return super().forget_if(key, expected_value)


class TestLockRelease:
    _KEY = "scheduler:lock:sweep"

    @pytest.fixture(autouse=True)
    def _no_retry_pause(self, monkeypatch) -> None:
        monkeypatch.setattr(driver_module, "_RELEASE_RETRY_DELAY_SECONDS", 0)

    def _locked(self, monkeypatch, cache, ran):
        monkeypatch.setattr(driver_module, "_Cache", cache)
        return driver_module._wrap_without_overlapping(
            "sweep", lambda: ran.append(1), lock_timeout=86400
        )

    def test_a_brief_blip_is_retried_within_the_fire(self, monkeypatch, log) -> None:
        ran: list[int] = []
        cache = _FlakyCache(blips=1)
        fire = self._locked(monkeypatch, cache, ran)

        fire()
        fire()

        assert ran == [1, 1]
        assert not cache.has(self._KEY)

    def test_a_lasting_failure_is_logged_and_cleared_by_the_next_fire(
        self, monkeypatch, log
    ) -> None:
        ran: list[int] = []
        cache = _FlakyCache(blips=driver_module._RELEASE_ATTEMPTS)
        fire = self._locked(monkeypatch, cache, ran)

        fire()
        assert cache.has(self._KEY)
        assert any(level == "warning" for level, _ in log.lines)

        fire()
        assert ran == [1, 1]
        assert not cache.has(self._KEY)

    def test_a_leftover_token_never_releases_a_peer_lock(self, monkeypatch, log) -> None:
        ran: list[int] = []
        cache = _FlakyCache(blips=driver_module._RELEASE_ATTEMPTS)
        fire = self._locked(monkeypatch, cache, ran)
        fire()
        # Our lock lapsed and a peer pod took the slot.
        cache.forget(self._KEY)
        cache.add(self._KEY, "peer-owner", 60)

        fire()

        assert ran == [1]
        assert cache.get(self._KEY) == "peer-owner"

    def test_a_skipped_fire_is_counted(self, monkeypatch, log, outcomes) -> None:
        cache = CacheFake()
        cache.add(self._KEY, "peer-owner", 60)
        fire = self._locked(monkeypatch, cache, [])

        assert fire() is None
        assert ("sweep", "skipped") in outcomes.seen


# ── 5. Executors ──────────────────────────────────────────────────────


class TestExecutors:
    @pytest.mark.parametrize(
        "options",
        [{"executor": "probez"}, {"apscheduler_job_options": {"executor": "probez"}}],
    )
    def test_an_undeclared_executor_is_refused_with_the_entry_name(
        self, driver, options
    ) -> None:
        with pytest.raises(InvalidArgumentException) as raised:
            _register(
                driver,
                "heartbeat",
                lambda: None,
                {"type": "interval", "seconds": 5},
                **options,
            )

        assert "heartbeat" in str(raised.value)
        assert "probez" in str(raised.value)

    @pytest.mark.parametrize("executor", ["probes", "default"])
    def test_a_declared_or_default_executor_is_accepted(self, driver, executor) -> None:
        _register(
            driver,
            "heartbeat",
            lambda: None,
            {"type": "interval", "seconds": 5},
            executor=executor,
        )

        assert driver._job_registry[0][3]["executor"] == executor


# ── 6. run_all failures ───────────────────────────────────────────────


def test_run_all_exposes_how_many_tasks_failed(driver) -> None:
    def boom() -> None:
        raise RuntimeError("task failed")

    _register(driver, "ok", lambda: None, {"type": "interval", "seconds": 5})
    _register(driver, "broken", boom, {"type": "interval", "seconds": 5})

    assert driver.run_all() == 1
    assert driver.last_run_failures == 1
    assert driver.last_run_failed_ids == ["broken"]
