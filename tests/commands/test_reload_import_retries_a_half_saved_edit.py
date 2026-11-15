"""A reloaded scheduler retries a job import that a half-saved edit broke.

The boot probe and the boot guard (``ProcessReload``) cover what
``bootstrap`` imports. ``schedule:work`` imports each scheduled job's module
later, in ``_register_jobs``, and only after ``reloaded_boot_completed``
dropped the guard. An edit saved across two files (the second one a moment
after the first) left the job's import raising ``ImportError`` for that
moment: the job was dropped with a warning and never registered again until
the next reload, and when every job failed the role exited on "No scheduled
jobs were registered". With auto-reload on, the import is now retried with a
short backoff (``IMPORT_SETTLE_SECONDS``) before it gives up.
"""

from __future__ import annotations

import importlib
import sys
from types import ModuleType, SimpleNamespace

import pytest

import cara.commands.core._ScheduleRegistration as schedule_registration
from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand
from cara.scheduling.ScheduleBuilder import ScheduleBuilder

reload_module = importlib.import_module("cara.commands.ProcessReload")

_MODULE = "_half_saved_probe_job"


class _Clock:
    """A fake monotonic clock that ``sleep`` advances."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _flaky_import(monkeypatch, failures: int, module: ModuleType | None = None):
    """``import_module`` raising ``ImportError`` ``failures`` times, then
    returning ``module``; the attempts are counted."""
    attempts: list[str] = []

    def fake(name: str, package: str | None = None):
        attempts.append(name)
        if len(attempts) <= failures:
            raise ImportError(f"cannot import name 'HALF' from {name!r}")
        return module if module is not None else ModuleType(name)

    monkeypatch.setattr(reload_module.importlib, "import_module", fake)
    return attempts


@pytest.fixture
def clock(monkeypatch) -> _Clock:
    fake = _Clock()
    monkeypatch.setattr(reload_module.time, "monotonic", fake.monotonic)
    monkeypatch.setattr(reload_module.time, "sleep", fake.sleep)
    return fake


def test_a_transient_import_error_is_retried_until_the_edit_lands(monkeypatch, clock):
    attempts = _flaky_import(monkeypatch, failures=3)

    module = reload_module.import_when_settled("app.jobs.FooJob")

    assert module.__name__ == "app.jobs.FooJob"
    assert len(attempts) == 4
    # A short, growing backoff — never a busy loop.
    assert clock.sleeps == sorted(clock.sleeps)
    assert all(
        0 < seconds <= reload_module.IMPORT_SETTLE_MAX_STEP for seconds in clock.sleeps
    )


def test_an_import_still_broken_after_the_budget_gives_up(monkeypatch, clock):
    _flaky_import(monkeypatch, failures=10_000)

    with pytest.raises(ImportError):
        reload_module.import_when_settled("app.jobs.FooJob")

    assert (
        clock.now
        <= reload_module.IMPORT_SETTLE_SECONDS + reload_module.IMPORT_SETTLE_MAX_STEP
    )
    assert clock.now >= reload_module.IMPORT_SETTLE_SECONDS


def test_a_half_saved_syntax_error_is_retried_too(monkeypatch, clock):
    calls = {"n": 0}

    def fake(name: str, package: str | None = None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise SyntaxError("'(' was never closed")
        return ModuleType(name)

    monkeypatch.setattr(reload_module.importlib, "import_module", fake)

    assert (
        reload_module.import_when_settled("app.jobs.FooJob").__name__ == "app.jobs.FooJob"
    )
    assert calls["n"] == 2


class _CentralJob:
    central_job = True

    def handle(self) -> None:
        return None


def _command(monkeypatch, *, reload_enabled: bool):
    driver_calls: list[str] = []

    class _Driver:
        def schedule_job(self, identifier, callback, schedule_spec, options) -> None:
            driver_calls.append(identifier)

    monkeypatch.setattr(
        schedule_registration,
        "Schedule",
        SimpleNamespace(
            call=lambda callback: ScheduleBuilder(_Driver(), "", callback, {})
        ),
    )
    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command.application = SimpleNamespace(make=lambda cls: cls())
    command.warning = lambda *_a, **_k: None
    command.info = lambda *_a, **_k: None
    command._auto_reload_enabled = reload_enabled
    return command, driver_calls


_SPEC = {
    "job": f"{_MODULE}._CentralJob",
    "id": "sweep_half_saved",
    "name": "Sweep",
    "trigger": "interval",
    "seconds": 30,
}


def test_a_reloading_scheduler_registers_a_job_whose_import_was_briefly_broken(
    monkeypatch, clock
):
    module = ModuleType(_MODULE)
    module._CentralJob = _CentralJob
    monkeypatch.delitem(sys.modules, _MODULE, raising=False)
    _flaky_import(monkeypatch, failures=2, module=module)
    command, registered = _command(monkeypatch, reload_enabled=True)

    entry = command._register_dict_job(dict(_SPEC))

    assert entry is not None
    assert registered == ["sweep_half_saved"]


def test_without_auto_reload_a_broken_import_fails_at_once(monkeypatch, clock):
    attempts = _flaky_import(monkeypatch, failures=1)
    command, registered = _command(monkeypatch, reload_enabled=False)

    with pytest.raises(ImportError):
        command._register_dict_job(dict(_SPEC))

    # Production boots never wait: one attempt, no sleep.
    assert len(attempts) == 1
    assert clock.sleeps == []
    assert registered == []


def test_the_boot_guard_stays_until_the_jobs_are_registered():
    """``reloaded_boot_completed`` runs once the jobs are in, not at the top
    of ``handle``: a registration that still fails after the retry goes back
    to the wait-for-a-bootable-tree loop instead of exiting the role."""
    import inspect

    handle = inspect.getsource(ScheduleWorkCommand.handle)
    loop = inspect.getsource(ScheduleWorkCommand._run_main_loop)
    assert "reloaded_boot_completed()" not in handle
    assert loop.index("reloaded_boot_completed()") > loop.index("_register_jobs()")
