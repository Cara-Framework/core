"""A tick the job's own gate refuses is SKIPPED, not failed.

The scheduler gave only ``ConcurrencyExceeded`` its throttle handling. A
``JobThrottledException`` — ``WithoutOverlapping`` finding the previous
tick's lock still held — fell into the generic ``except BaseException``:
the tracked tick row was written ``failed`` and the exception escaped to the
driver, which counted ``outcome="failure"``. Being skipped because the
previous run is still going is the driver's own ``skipped`` outcome: the
registration calls the tick row off and hands the refusal to the driver,
which counts the fire once, as skipped (swallowing it in the registration
made the driver count the same fire a success as well).
"""

from __future__ import annotations

import importlib
import sys
from types import ModuleType, SimpleNamespace

import pytest

import cara.commands.core._ScheduleRegistration as schedule_registration
from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand
from cara.queues import JobThrottledException
from cara.scheduling.ScheduleBuilder import ScheduleBuilder

# The module, not the class of the same name the package barrel exports.
scheduler_driver = importlib.import_module("cara.scheduling.drivers.APSchedulerDriver")

_PROBE_MODULE = "_throttled_tick_probe_job"


class _OverlappingJob:
    """A central job whose previous tick still holds the overlap lock."""

    central_job = True
    error: Exception = JobThrottledException("still running", key="probe")

    async def handle(self) -> None:
        raise _OverlappingJob.error


class _Tracker:
    """A ``JobTracker`` double recording every settlement."""

    def __init__(self) -> None:
        self.statuses: list[str] = []
        self.throttled: list[str] = []

    def create_job_record(self, **_kwargs) -> int:
        return 41

    def update_job_status(self, _job_id: int, status: str) -> None:
        self.statuses.append(status)

    def track_job_throttled(self, _job_id: int, reason: str | None = None) -> None:
        self.throttled.append(reason or "")


class _Driver:
    """Keeps the registered callback so the test can fire one tick."""

    def __init__(self) -> None:
        self.callback = None

    def schedule_job(self, _identifier, callback, _spec, _options) -> None:
        self.callback = callback


class _Counter:
    def __init__(self) -> None:
        self.outcomes: list[str] = []

    def labels(self, task: str, outcome: str):
        self.outcomes.append(outcome)
        return SimpleNamespace(inc=lambda: None)


@pytest.fixture
def fire(monkeypatch):
    module = ModuleType(_PROBE_MODULE)
    module._OverlappingJob = _OverlappingJob
    monkeypatch.setitem(sys.modules, _PROBE_MODULE, module)

    driver, tracker, counter = _Driver(), _Tracker(), _Counter()
    monkeypatch.setattr(
        schedule_registration,
        "Schedule",
        SimpleNamespace(call=lambda callback: ScheduleBuilder(driver, "", callback, {})),
    )
    monkeypatch.setattr(
        schedule_registration,
        "config",
        lambda key, default=None: True if key == "scheduling.track_ticks" else default,
    )
    monkeypatch.setattr(
        scheduler_driver,
        "MetricsBase",
        SimpleNamespace(scheduled_tasks_total=counter),
    )

    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command.application = SimpleNamespace(
        has=lambda key: key == "JobTracker",
        make=lambda key: tracker if key == "JobTracker" else key(),
    )
    command.warning = lambda *_args, **_kwargs: None
    command.info = lambda *_args, **_kwargs: None
    command._register_dict_job(
        {
            "job": f"{_PROBE_MODULE}._OverlappingJob",
            "id": "sweep_inbox",
            "trigger": "interval",
            "seconds": 30,
        }
    )
    # ``run`` is the tick as the engine fires it: through the driver's wrapper.
    run = scheduler_driver._instrument_scheduled("sweep_inbox", driver.callback)
    return SimpleNamespace(
        run=run, registered=driver.callback, tracker=tracker, counter=counter
    )


def test_a_throttled_tick_is_not_an_error_for_the_engine(fire):
    assert fire.run() is None


def test_the_registration_hands_the_refusal_to_the_driver(fire):
    with pytest.raises(JobThrottledException):
        fire.registered()


def test_the_tick_row_is_called_off_not_failed(fire):
    fire.run()
    assert "failed" not in fire.tracker.statuses
    assert fire.tracker.throttled and fire.tracker.throttled[0].startswith("skipped")


def test_the_fire_counts_once_and_as_skipped(fire):
    fire.run()
    assert fire.counter.outcomes == ["skipped"]


def test_a_real_failure_still_fails(fire, monkeypatch):
    monkeypatch.setattr(_OverlappingJob, "error", RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        fire.run()
    assert fire.tracker.statuses[-1] == "failed"
    assert fire.tracker.throttled == []
    assert fire.counter.outcomes == ["failure"]
