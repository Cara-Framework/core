"""``"dispatch": True`` makes a schedule entry queue its job, not run it inline.

Every tick runs inline in ``schedule:work`` (no kill timer): a long or
remote-calling tick holds a pool thread and the reload for its whole run, and
the other ticks miss their fire. A dispatching entry hands the job to a worker.
"""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import pytest

import cara.commands.core._ScheduleRegistration as schedule_registration
from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand
from cara.context import Tenancy
from cara.scheduling.ScheduleBuilder import ScheduleBuilder

_PROBE_MODULE = "_dispatch_probe_job"


class _Job:
    central_job = True
    ran = 0

    def __init__(self, limit: int = 1) -> None:
        self.limit = limit

    def handle(self) -> None:
        type(self).ran += 1


class _Driver:
    def __init__(self) -> None:
        self.callbacks = []

    def schedule_job(self, identifier, callback, schedule_spec, options) -> None:
        self.callbacks.append(callback)


@pytest.fixture
def tick(monkeypatch):
    module = ModuleType(_PROBE_MODULE)
    module._Job = _Job
    monkeypatch.setitem(sys.modules, _PROBE_MODULE, module)
    driver = _Driver()
    monkeypatch.setattr(
        schedule_registration,
        "Schedule",
        SimpleNamespace(call=lambda callback: ScheduleBuilder(driver, "", callback, {})),
    )
    queued = []

    async def _dispatch(job):
        queued.append((job, Tenancy.is_central()))

    monkeypatch.setattr(schedule_registration, "Bus", SimpleNamespace(dispatch=_dispatch))
    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command.application = SimpleNamespace(make=lambda cls: cls())
    command.warning = lambda *_a, **_k: None
    _Job.ran = 0

    def _tick(extra: dict):
        command._register_dict_job(
            {
                "job": f"{_PROBE_MODULE}._Job",
                "id": "sweep",
                "trigger": "cron",
                "hour": 3,
                "minute": 0,
                **extra,
            }
        )
        driver.callbacks[-1]()
        return queued

    return _tick


def test_a_dispatching_entry_queues_its_job_under_its_scope(tick) -> None:
    queued = tick({"dispatch": True, "kwargs": {"limit": 7}})
    ((job, central),) = queued
    assert isinstance(job, _Job) and job.limit == 7 and central
    assert _Job.ran == 0
    # The scheduler's result-cache opt-out rides the queued job too.
    assert job.idempotency_cache_results is False


def test_an_inline_entry_still_runs_in_the_tick(tick) -> None:
    assert tick({}) == []
    assert _Job.ran == 1
