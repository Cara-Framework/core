"""Every dict schedule entry is registered on the wall clock.

Live 2026-10-30 (Synkronus): the scheduler restarted three times in seven
minutes and each start re-anchored every interval to boot, so a 15-minute
sweep due at 21:28 fired first at 21:45, and a 6h/12h/24h interval on a
scheduler restarted more often than that never ran. An "hourly at :MM" cron
declared without an hour ran once a day at 00:MM.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from types import ModuleType, SimpleNamespace

import pytest

import cara.commands.core._ScheduleRegistration as schedule_registration
from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand
from cara.scheduling import declared_interval_seconds, on_wall_clock
from cara.scheduling.drivers.APSchedulerDriver import APSchedulerDriver
from cara.scheduling.ScheduleBuilder import ScheduleBuilder

_PROBE_MODULE = "_wall_clock_probe_job"


class _ProbeJob:
    central_job = True

    def handle(self) -> None:
        return None


class _RecordingDriver:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict, dict]] = []

    def schedule_job(self, identifier, callback, schedule_spec, options) -> None:
        self.calls.append((identifier, dict(schedule_spec), dict(options)))


def _trigger(spec: dict):
    driver = APSchedulerDriver.__new__(APSchedulerDriver)
    driver._settings = {"timezone": "UTC"}
    return driver._build_trigger(spec)


def _next(trigger, at: str) -> datetime:
    return trigger.get_next_fire_time(
        None, datetime.fromisoformat(at).replace(tzinfo=UTC)
    )


def test_a_quarter_hour_interval_becomes_its_cron_and_keeps_its_cadence():
    entry = on_wall_clock({"trigger": "interval", "minutes": 15, "id": "drift"})
    assert (entry["trigger"], entry["hour"], entry["minute"]) == ("cron", "*", "*/15")
    assert entry["minutes"] == 15
    trigger = _trigger(
        {"type": "daily", "hour": entry["hour"], "minute": entry["minute"]}
    )
    # Booted at 18:30:26: next fire 18:45, not boot + 15 minutes.
    assert _next(trigger, "2026-10-30T18:30:26") == datetime(
        2026, 10, 30, 18, 45, tzinfo=UTC
    )
    assert _next(trigger, "2026-10-30T18:44:59") == datetime(
        2026, 10, 30, 18, 45, tzinfo=UTC
    )


def test_hour_grained_and_daily_intervals_get_a_stable_slot():
    six = on_wall_clock({"trigger": "interval", "hours": 6, "id": "audit"})
    assert (six["trigger"], six["hour"]) == ("cron", "*/6")
    assert 0 <= int(six["minute"]) < 60
    assert on_wall_clock({"trigger": "interval", "hours": 6, "id": "audit"}) == six
    daily = on_wall_clock({"trigger": "interval", "hours": 24, "id": "fees"})
    assert (daily["trigger"], daily["hour"]) == ("cron", "0")
    assert on_wall_clock({"trigger": "interval", "days": 1, "id": "fees"})["hour"] == "0"
    assert (
        on_wall_clock({"trigger": "interval", "minutes": 1, "id": "x"})["minute"] == "*"
    )


def test_a_period_the_clock_cannot_divide_stays_an_interval():
    for spec in ({"seconds": 30}, {"minutes": 45}, {"hours": 5}, {"hours": 48}):
        entry = {"trigger": "interval", "id": "x", **spec}
        assert on_wall_clock(entry) == entry


def test_an_hour_less_cron_runs_hourly_and_a_full_cron_is_untouched():
    assert on_wall_clock({"trigger": "cron", "minute": 5, "id": "x"})["hour"] == "*"
    daily = {"trigger": "cron", "hour": 3, "minute": 5, "id": "x"}
    weekly = {"trigger": "cron", "day_of_week": "mon", "minute": 5, "id": "x"}
    assert on_wall_clock(daily) == daily
    assert on_wall_clock(weekly) == weekly


def test_on_wall_clock_is_idempotent():
    once = on_wall_clock({"trigger": "interval", "hours": 12, "id": "crawl"})
    assert on_wall_clock(once) == once


def test_declared_interval_seconds():
    assert declared_interval_seconds({"hours": 1, "minutes": 30}) == 5400
    assert declared_interval_seconds({"trigger": "cron", "hour": 3}) is None
    assert declared_interval_seconds({"minutes": "x"}) is None


@pytest.fixture
def register(monkeypatch):
    module = ModuleType(_PROBE_MODULE)
    module._ProbeJob = _ProbeJob
    monkeypatch.setitem(sys.modules, _PROBE_MODULE, module)
    driver = _RecordingDriver()
    monkeypatch.setattr(
        schedule_registration,
        "Schedule",
        SimpleNamespace(call=lambda callback: ScheduleBuilder(driver, "", callback, {})),
    )
    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command.application = SimpleNamespace(make=lambda cls: cls())
    command.warning = lambda *_a, **_k: None
    command.info = lambda *_a, **_k: None

    def _register(extra: dict) -> dict:
        command._register_dict_job(
            {"job": f"{_PROBE_MODULE}._ProbeJob", "id": "sweep", "name": "Sweep", **extra}
        )
        return driver.calls[-1][1]

    return _register


def test_the_registration_hands_the_driver_a_cron_for_a_six_hour_interval(register):
    spec = register({"trigger": "interval", "hours": 6})
    assert spec["type"] != "interval"
    trigger = _trigger(spec)
    first = _next(trigger, "2026-10-30T10:00:00")
    second = trigger.get_next_fire_time(first, first)
    assert (second - first).total_seconds() == 6 * 3600
    assert first.hour % 6 == 0


def test_the_registration_runs_an_hour_less_cron_every_hour(register):
    trigger = _trigger(register({"trigger": "cron", "minute": 7}))
    first = _next(trigger, "2026-10-30T10:00:00")
    second = trigger.get_next_fire_time(first, first)
    assert (first.minute, (second - first).total_seconds()) == (7, 3600)


def test_a_sub_minute_tick_is_still_registered_as_an_interval(register):
    assert register({"trigger": "interval", "seconds": 30})["type"] == "interval"
