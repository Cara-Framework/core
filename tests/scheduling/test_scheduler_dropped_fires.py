"""A scheduled fire the engine drops must be visible, and a restart must not
silently lose one.

APScheduler keeps its job store in memory, so every boot computes next runs
from boot time: a daily cron whose minute falls inside a deploy does not run
that day, and a 24h interval re-anchors to boot. A run that waits past the
misfire grace time for an executor thread is dropped before it reaches the
callback. These tests pin the catch-up memory, the dropped-fire metric and
the executor passthrough that close those gaps.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from types import ModuleType, SimpleNamespace

import pytest

import cara.commands.core._ScheduleRegistration as schedule_registration
from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand
from cara.configuration import Configuration
from cara.scheduling.drivers.APSchedulerDriver import APSchedulerDriver
from cara.scheduling.ScheduleBuilder import ScheduleBuilder

driver_module = sys.modules["cara.scheduling.drivers.APSchedulerDriver"]

_DAY = timedelta(days=1)


@pytest.fixture(autouse=True)
def _isolated_configuration(monkeypatch):
    monkeypatch.setattr(Configuration, "_instance", None)
    Configuration.empty()


class _MemoryCache:
    def __init__(self, values: dict | None = None) -> None:
        self.values = dict(values or {})
        self.puts: list[tuple[str, object, int]] = []

    def get(self, key):
        return self.values.get(key)

    def put(self, key, value, ttl):
        self.puts.append((key, value, ttl))
        self.values[key] = value


class _EveryDay:
    """The interval semantics the engine applies: previous fire + period."""

    def get_next_fire_time(self, previous_fire_time, _now):
        return previous_fire_time + _DAY


class _EngineFreeDriver(APSchedulerDriver):
    def _create_scheduler(self):
        added: list[dict] = []
        return SimpleNamespace(
            running=False,
            added=added,
            add_job=lambda **kwargs: added.append(kwargs),
            remove_job=lambda **_kwargs: None,
            start=lambda: None,
        )

    def _build_trigger(self, spec):
        return _EveryDay()


def _stamp(ago: timedelta) -> float:
    return (datetime.now(UTC) - ago).timestamp()


# ── Catch-up memory ────────────────────────────────────────────────────


def test_a_fire_that_fell_inside_the_downtime_runs_on_boot(monkeypatch) -> None:
    cache = _MemoryCache({"scheduler:last_fire:close": _stamp(_DAY + timedelta(hours=1))})
    monkeypatch.setattr(driver_module, "_Cache", cache)

    first_run = driver_module._catch_up_run_time("close", _EveryDay())

    assert first_run is not None
    assert abs((first_run - datetime.now(UTC)).total_seconds()) < 5


def test_a_restart_before_the_next_fire_keeps_the_remembered_cadence(
    monkeypatch,
) -> None:
    last_fire = _stamp(timedelta(hours=6))
    monkeypatch.setattr(
        driver_module,
        "_Cache",
        _MemoryCache({"scheduler:last_fire:finances": last_fire}),
    )

    first_run = driver_module._catch_up_run_time("finances", _EveryDay())

    assert first_run == datetime.fromtimestamp(last_fire, tz=UTC) + _DAY


def test_nothing_remembered_leaves_the_engine_next_run(monkeypatch) -> None:
    monkeypatch.setattr(driver_module, "_Cache", _MemoryCache())

    assert driver_module._catch_up_run_time("close", _EveryDay()) is None


def test_the_stamp_is_the_fire_start_written_after_success_or_failure(
    monkeypatch,
) -> None:
    cache = _MemoryCache()
    monkeypatch.setattr(driver_module, "_Cache", cache)
    before = datetime.now(UTC).timestamp()

    driver_module._wrap_catch_up("close", lambda: None)()

    def _failing():
        raise RuntimeError("provider down")

    with pytest.raises(RuntimeError):
        driver_module._wrap_catch_up("close", _failing)()

    assert [key for key, _value, _ttl in cache.puts] == [
        "scheduler:last_fire:close",
        "scheduler:last_fire:close",
    ]
    assert all(
        before <= value <= datetime.now(UTC).timestamp() for _k, value, _t in cache.puts
    )
    assert all(ttl >= 366 * 86400 for _k, _v, ttl in cache.puts)


def test_only_catch_up_entries_get_a_remembered_first_run(monkeypatch) -> None:
    monkeypatch.setattr(
        driver_module,
        "_Cache",
        _MemoryCache(
            {
                "scheduler:last_fire:close": _stamp(_DAY * 2),
                "scheduler:last_fire:plain": _stamp(_DAY * 2),
            }
        ),
    )
    driver = _EngineFreeDriver()
    spec = {"type": "interval", "hours": 24}
    driver.schedule_job("close", lambda: None, spec, {"silent": True, "catch_up": True})
    driver.schedule_job("plain", lambda: None, spec, {"silent": True})

    driver.start()

    added = {job["id"]: job for job in driver.scheduler.added}
    assert "next_run_time" in added["close"]
    assert "next_run_time" not in added["plain"]


# ── Dropped fires are counted ──────────────────────────────────────────


class _Counter:
    def __init__(self) -> None:
        self.labels_seen: list[dict] = []

    def labels(self, **labels):
        self.labels_seen.append(labels)
        return SimpleNamespace(inc=lambda: None)


def test_missed_and_skipped_fires_reach_the_task_counter(monkeypatch) -> None:
    counter = _Counter()
    monkeypatch.setattr(
        driver_module,
        "MetricsBase",
        SimpleNamespace(scheduled_tasks_total=counter),
    )
    event = SimpleNamespace(job_id="close_billing_periods", scheduled_run_time=None)

    driver_module._job_missed_listener(event)
    driver_module._job_max_instances_listener(event)

    assert counter.labels_seen == [
        {"task": "close_billing_periods", "outcome": "missed"},
        {"task": "close_billing_periods", "outcome": "skipped"},
    ]


# ── Executor passthrough and restart-safe settings ─────────────────────


class _RecordingDriver:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict, dict]] = []

    def schedule_job(self, identifier, callback, schedule_spec, options) -> None:
        self.calls.append((identifier, dict(schedule_spec), dict(options)))


def test_a_dict_entry_forwards_catch_up_and_executor(monkeypatch) -> None:
    module = ModuleType("_dropped_fire_probe_job")

    class _ProbeJob:
        central_job = True

        def handle(self) -> None:
            return None

    module._ProbeJob = _ProbeJob
    monkeypatch.setitem(sys.modules, "_dropped_fire_probe_job", module)
    recording = _RecordingDriver()
    monkeypatch.setattr(
        schedule_registration,
        "Schedule",
        SimpleNamespace(
            call=lambda callback: ScheduleBuilder(recording, "", callback, {})
        ),
    )
    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command.application = SimpleNamespace(make=lambda cls: cls())

    command._register_dict_job(
        {
            "job": "_dropped_fire_probe_job._ProbeJob",
            "id": "sweep_queue_outbox_health",
            "trigger": "interval",
            "minutes": 1,
            "catch_up": True,
            "executor": "probes",
        }
    )

    _identifier, _spec, options = recording.calls[0]
    assert options["catch_up"] is True
    assert options["executor"] == "probes"


def test_the_driver_puts_a_named_executor_on_the_engine_job() -> None:
    driver = _EngineFreeDriver()
    driver.schedule_job(
        "sweep_queue_outbox_health",
        lambda: None,
        {"type": "interval", "minutes": 1},
        {"silent": True, "executor": "probes"},
    )

    driver.start()

    assert driver.scheduler.added[0]["executor"] == "probes"


def test_a_restart_rebuilds_executors_from_unmutated_settings(monkeypatch) -> None:
    """The engine pops ``type`` out of each executor dict it configures."""
    built: list[dict] = []

    class _PoppingScheduler:
        running = False

        def __init__(self, **kwargs):
            for value in kwargs.get("executors", {}).values():
                if not value.pop("type", None):
                    raise ValueError("executor must define type")
            built.append(kwargs)

        def add_listener(self, *_args):
            return None

    background = ModuleType("apscheduler.schedulers.background")
    background.BackgroundScheduler = _PoppingScheduler
    for name, value in (
        ("apscheduler", ModuleType("apscheduler")),
        ("apscheduler.schedulers", ModuleType("apscheduler.schedulers")),
        ("apscheduler.schedulers.background", background),
    ):
        monkeypatch.setitem(sys.modules, name, value)
    settings = {"executors": {"probes": {"type": "threadpool", "max_workers": 2}}}

    driver = APSchedulerDriver(settings)
    driver._create_scheduler()

    assert len(built) == 2
    assert settings["executors"]["probes"] == {"type": "threadpool", "max_workers": 2}
