"""``schedule:work --once`` exit code, and ``days`` / ``weeks`` interval entries.

Two defects in what the schedule worker told its caller:

  * ``--once`` printed "(N succeeded)" and exited 0 whatever happened: the
    driver's ``run_all`` logs a failing task and carries on, so a cron wrapper
    saw a clean pass over tasks that raised. The driver now reports the
    failures (``last_run_failures`` / ``last_run_failed_ids``) and the command
    exits 1 on them — never on the success count, which a fire skipped by its
    overlap lock also lowers.
  * a dict entry's ``days`` / ``weeks`` never reached the scheduler: the entry
    was registered with a zero period, which APScheduler ran every second.
"""

from __future__ import annotations

import importlib
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import cara.commands.core._ScheduleRegistration as schedule_registration
from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand


def _run_once(monkeypatch, **driver_report):
    """One ``--once`` pass over a driver that leaves ``driver_report`` behind."""
    module = importlib.import_module("cara.commands.core.ScheduleWorkCommand")
    driver = SimpleNamespace(
        run_all=lambda: 1, shutdown=lambda wait=True: None, **driver_report
    )
    monkeypatch.setattr(module, "Schedule", SimpleNamespace(driver=lambda _name: driver))

    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command.console = MagicMock()
    command.shutdown_requested = False
    command._reload_requested = False
    command._show_scheduler_status = lambda: None
    errors: list[str] = []
    command.error = errors.append
    command._start_scheduler({"driver_name": "fake", "run_once": True})
    return command._exit_code, errors


def test_once_exits_non_zero_when_the_driver_reports_failures(monkeypatch) -> None:
    exit_code, errors = _run_once(
        monkeypatch, last_run_failures=2, last_run_failed_ids=["sweep", "digest"]
    )
    assert exit_code == 1
    assert errors == ["× 2 scheduled task(s) failed: sweep, digest"]


def test_once_exits_zero_on_a_pass_without_failures(monkeypatch) -> None:
    """One success of several registered tasks is not a failure by itself: a
    fire skipped by its overlap lock lowers the count and failed nothing."""
    exit_code, errors = _run_once(
        monkeypatch, last_run_failures=0, last_run_failed_ids=[]
    )
    assert exit_code is None
    assert errors == []


def test_a_driver_without_the_failure_report_keeps_exit_zero(monkeypatch) -> None:
    """The report is the APScheduler driver's; the contract only promises the
    success count, and a driver that says nothing more must not start failing."""
    exit_code, errors = _run_once(monkeypatch)
    assert exit_code is None
    assert errors == []


def test_the_long_running_worker_reports_no_exit_code() -> None:
    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    assert command._exit_code is None


# ── dict entries: days / weeks ──────────────────────────────────────────────

_PROBE_MODULE = "_long_interval_probe_job"


class _CentralProbeJob:
    central_job = True

    def handle(self) -> None:
        return None


class _RecordingBuilder:
    """Stands in for ``ScheduleBuilder``: records the terminal ``interval``."""

    def __init__(self) -> None:
        self.identifier = ""
        self.options: dict = {}
        self.interval_kwargs: dict | None = None

    def interval(self, **kwargs) -> None:
        self.interval_kwargs = kwargs


def _register(monkeypatch, **cadence) -> tuple[dict, dict]:
    module = ModuleType(_PROBE_MODULE)
    module._CentralProbeJob = _CentralProbeJob
    monkeypatch.setitem(sys.modules, _PROBE_MODULE, module)
    builder = _RecordingBuilder()
    monkeypatch.setattr(
        schedule_registration, "Schedule", SimpleNamespace(call=lambda _cb: builder)
    )
    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command.application = SimpleNamespace(make=lambda cls: cls())
    command.warning = lambda *_a, **_k: None
    command.info = lambda *_a, **_k: None
    entry = command._register_dict_job(
        {
            "job": f"{_PROBE_MODULE}._CentralProbeJob",
            "id": "long_sweep",
            "name": "Long sweep",
            "trigger": "interval",
            **cadence,
        }
    )
    return builder.interval_kwargs, entry


def _cadence(**declared) -> dict:
    return {"seconds": 0, "minutes": 0, "hours": 0, "days": 0, "weeks": 0, **declared}


def test_days_and_weeks_reach_the_scheduler(monkeypatch) -> None:
    """``{"days": 2}`` used to register as seconds=minutes=hours=0."""
    kwargs, entry = _register(monkeypatch, days=2)
    assert kwargs == _cadence(days=2)
    assert entry["schedule"] == "Every 2d"

    kwargs, entry = _register(monkeypatch, days=2, hours=6)
    assert kwargs == _cadence(days=2, hours=6)
    assert entry["schedule"] == "Every 2d 6h"

    kwargs, entry = _register(monkeypatch, weeks=1)
    assert kwargs == _cadence(weeks=1)
    assert entry["schedule"] == "Every 1w"


def test_a_short_interval_is_forwarded_unchanged(monkeypatch) -> None:
    kwargs, entry = _register(monkeypatch, seconds=45)
    assert kwargs == _cadence(seconds=45)
    assert entry["schedule"] == "Every 45s"


def test_the_real_builder_takes_what_the_registration_forwards() -> None:
    """Pins the two sides together: a keyword the builder does not accept
    would stop every interval entry at registration."""
    import inspect

    from cara.scheduling.ScheduleBuilder import ScheduleBuilder

    accepted = set(inspect.signature(ScheduleBuilder.interval).parameters)
    assert set(_cadence()) <= accepted
