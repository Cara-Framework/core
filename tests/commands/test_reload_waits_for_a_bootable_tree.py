"""A reloading role replaces itself only with a tree that boots.

``schedule:work`` and ``queue:work`` reload by stopping, then replacing their
process with ``os.execv`` so every provider re-bootstraps. A file change saved
mid-edit replaced the role with an interpreter that died at
``from bootstrap import application`` — and nothing started it again. Live
2026-11-11 20:04: a services edit left a circular import for a moment, the
scheduler re-executed into it and stayed down until a manual restart at 01:51
(5h47m with no recurring sync; the API's heartbeat alert fired at 20:14).

The replacement now probes the tree first (the same entry point with
``--help``, which imports and registers what a real start does), reports a
broken tree once and probes again until it boots.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from cara.commands.core.QueueWorkCommand import QueueWorkCommand
from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand


class _World:
    """``subprocess.run`` answers from a script; ``execv``/``sleep`` record."""

    def __init__(self, outcomes: list[tuple[int, str]]):
        self.outcomes = list(outcomes)
        self.events: list[tuple] = []

    def run(self, argv, **_kwargs):
        returncode, stderr = self.outcomes.pop(0)
        self.events.append(("probe", list(argv)))
        return SimpleNamespace(returncode=returncode, stdout="", stderr=stderr)

    def execv(self, path, argv):
        self.events.append(("execv", path, list(argv)))

    def sleep(self, seconds):
        self.events.append(("sleep", seconds))


@pytest.fixture
def world(monkeypatch):
    def make(outcomes):
        current = _World(outcomes)
        monkeypatch.setattr(subprocess, "run", current.run)
        monkeypatch.setattr(os, "execv", current.execv)
        monkeypatch.setattr(time, "sleep", current.sleep)
        monkeypatch.setattr(sys, "argv", ["craft", "schedule:work"])
        # The wait restores default signal dispositions; keep the runner's own.
        monkeypatch.setattr(signal, "signal", lambda *_a, **_k: None)
        return current

    return make


BROKEN = (
    1,
    "Traceback (most recent call last):\n  ...\nImportError: cannot import name "
    "'connector_availability' from partially initialized module\n",
)


@pytest.mark.parametrize(
    "restart",
    [
        ScheduleWorkCommand._restart_scheduler_process,
        QueueWorkCommand._restart_worker_process,
    ],
)
def test_a_tree_that_does_not_boot_is_not_executed_into(world, restart):
    """Pinned wrong behaviour: ``execv`` ran at once, into the broken tree."""
    current = world([BROKEN, BROKEN, (0, "")])

    restart()

    kinds = [event[0] for event in current.events]
    assert kinds == ["probe", "sleep", "probe", "sleep", "probe", "execv"]
    probe = current.events[0][1]
    assert probe == [sys.executable, "craft", "--help"]
    assert current.events[-1] == (
        "execv",
        sys.executable,
        [sys.executable, "craft", "schedule:work"],
    )


def test_a_bootable_tree_is_executed_into_at_once(world):
    current = world([(0, "")])

    ScheduleWorkCommand._restart_scheduler_process()

    assert [event[0] for event in current.events] == ["probe", "execv"]
