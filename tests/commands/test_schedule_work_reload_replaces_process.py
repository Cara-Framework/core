"""schedule:work reloads by replacing the process, never in-process.

The generic ``MakesAutoReload`` path purged ``app.*`` and re-entered the
loop without re-running the service providers, so every boot-time registry
(automation actions, sweep lanes, container bindings) came back empty
(live test B, 2026-10-28: "unknown action adjust_price", a sweep job failing
every minute). A file change now only stops the scheduler; ``handle`` then
replaces the process (``os.execv``), which re-bootstraps everything.
"""

from __future__ import annotations

import importlib
import sys

from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand

# The package barrel exports the class under the module's name.
module = importlib.import_module("cara.commands.core.ScheduleWorkCommand")


def _command() -> ScheduleWorkCommand:
    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command.shutdown_requested = False
    command._auto_reload_enabled = True
    command._reload_requested = False
    command._observer = None
    command.info = lambda *_a, **_k: None
    return command


def test_a_file_change_stops_the_loop_and_purges_nothing(monkeypatch):
    command = _command()
    before = set(sys.modules)
    command._trigger_auto_reload()
    assert command.shutdown_requested is True
    assert command._reload_requested is True
    # No module purge, no in-process re-entry of the loop.
    assert set(sys.modules) >= before


def test_a_second_change_while_stopping_is_ignored():
    command = _command()
    command._trigger_auto_reload()
    command.shutdown_requested = False  # would be re-set by a second trigger
    command._trigger_auto_reload()
    assert command.shutdown_requested is False


def test_disabled_reload_does_nothing():
    command = _command()
    command._auto_reload_enabled = False
    command._trigger_auto_reload()
    assert command._reload_requested is False


def test_the_stopped_scheduler_is_replaced_with_the_same_arguments(monkeypatch):
    calls = []
    monkeypatch.setattr(module.os, "execv", lambda path, argv: calls.append((path, argv)))
    monkeypatch.setattr(sys, "argv", ["craft", "schedule:work"])
    ScheduleWorkCommand._restart_scheduler_process()
    assert calls == [(sys.executable, [sys.executable, "craft", "schedule:work"])]
