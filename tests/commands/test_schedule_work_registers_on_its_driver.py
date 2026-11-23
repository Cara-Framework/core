"""``schedule:work --driver X`` registers its jobs on X, not the default.

Live defect in ``cara/commands/core/ScheduleWorkCommand.py``: the command
ran driver X, but ``_register_jobs`` went through ``Schedule.call()``, which
always registered on the DEFAULT driver, so X started with no jobs at all.
The command now calls ``Schedule.use(<its driver name>)`` before any
registration.
"""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock

import pytest

from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand
from cara.exceptions import DriverNotRegisteredException

module = importlib.import_module("cara.commands.core.ScheduleWorkCommand")


class _Schedule:
    def __init__(self, events: list, known: set[str]) -> None:
        self.events = events
        self.known = known

    def use(self, name):
        if name not in self.known:
            raise DriverNotRegisteredException(f"{name} is not registered")
        self.events.append(("use", name))
        return self


def _command(monkeypatch, events: list, known: set[str]) -> ScheduleWorkCommand:
    monkeypatch.setattr(module, "Schedule", _Schedule(events, known))
    monkeypatch.setattr(module, "_Metrics", MagicMock())
    monkeypatch.setattr(module, "reloaded_boot_completed", lambda: None)

    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command._restart_params = None
    command.console = MagicMock()
    command.option = lambda _name: False
    command.info = lambda *_a, **_k: None
    command.error = lambda *_a, **_k: None
    command._show_config = lambda _cfg: None
    command._show_jobs = lambda _entries: None

    def register():
        events.append(("register",))
        return [{"id": "sweep"}]

    command._register_jobs = register
    command._start_scheduler = lambda cfg: events.append(("start", cfg["driver_name"]))
    return command


def test_the_driver_is_selected_before_any_job_registers(monkeypatch) -> None:
    events: list = []
    command = _command(monkeypatch, events, known={"default", "secondary"})

    command._run_main_loop("secondary")

    assert events == [("use", "secondary"), ("register",), ("start", "secondary")]


def test_an_unregistered_driver_is_refused_before_registering(monkeypatch) -> None:
    events: list = []
    command = _command(monkeypatch, events, known={"default"})

    with pytest.raises(DriverNotRegisteredException):
        command._run_main_loop("missing")

    assert events == []
