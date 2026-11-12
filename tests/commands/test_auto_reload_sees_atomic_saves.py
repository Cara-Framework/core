"""Auto-reload sees a source file that was saved by replacing it.

``MakesAutoReload``'s watcher only answered ``on_modified``. An editor or tool
that saves atomically — writes ``Name.py.tmp.<pid>.<id>`` beside the file,
then renames it over ``Name.py`` (the agents' Edit/Write tools do exactly
this) — produces, on macOS FSEvents, ``created``/``modified`` for the temp
name, ``moved`` temp -> ``Name.py`` and a ``deleted`` for ``Name.py``, and NO
``modified`` for ``Name.py``. The temp name does not end in ``.py``, so every
such save was ignored: live 2026-11-12, ``schedule:work`` reloaded at 11:28
and then missed 30+ saves under ``app/`` and ``packages/`` until 12:05,
dispatching job shapes the edited tree no longer had (role skew, dead letters
of ``VerifyChannelSyncTasksJob`` / ``RefreshListingObservedJob``).

A rename onto a watched file, a new file and a removed file change the code
the process would import just as an in-place write does.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from watchdog.events import (
    DirModifiedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
)

from cara.commands.MakesAutoReload import _auto_reload_handler


def _handler(debounce_delay: float = 0.0):
    triggers: list[str] = []
    command = SimpleNamespace(
        info=lambda *_a, **_k: None,
        _trigger_auto_reload=lambda: triggers.append("reload"),
    )
    # No debounce by default: each case below is one save.
    return _auto_reload_handler(command, debounce_delay=debounce_delay), triggers


@pytest.mark.parametrize(
    "event",
    [
        FileMovedEvent(
            "/srv/app/jobs/Job.py.tmp.88971.1b3e9dc04186", "/srv/app/jobs/Job.py"
        ),
        FileCreatedEvent("/srv/app/jobs/NewJob.py"),
        FileDeletedEvent("/srv/app/jobs/RemovedJob.py"),
        FileModifiedEvent("/srv/app/jobs/Job.py"),
    ],
    ids=["atomic-rename-onto", "created", "deleted", "modified-in-place"],
)
def test_a_source_change_of_any_kind_reloads(event):
    handler, triggers = _handler()

    handler.dispatch(event)

    assert triggers == ["reload"]


@pytest.mark.parametrize(
    "event",
    [
        # The temp half of an atomic save is not source.
        FileCreatedEvent("/srv/app/jobs/Job.py.tmp.88971.1b3e9dc04186"),
        FileModifiedEvent("/srv/app/jobs/Job.py.tmp.88971.1b3e9dc04186"),
        DirModifiedEvent("/srv/app/jobs"),
        FileModifiedEvent("/srv/app/jobs/__pycache__/Job.cpython-314.pyc"),
        FileModifiedEvent("/srv/app/README.md"),
    ],
    ids=["tmp-created", "tmp-modified", "directory", "bytecode", "not-watched-ext"],
)
def test_non_source_events_do_not_reload(event):
    handler, triggers = _handler()

    handler.dispatch(event)

    assert triggers == []


def test_a_source_file_renamed_away_reloads():
    handler, triggers = _handler()

    handler.dispatch(FileMovedEvent("/srv/app/jobs/Old.py", "/srv/app/jobs/Old.py.bak"))

    assert triggers == ["reload"]


def test_the_events_of_one_atomic_save_reload_once():
    handler, triggers = _handler(debounce_delay=0.3)

    for event in (
        FileCreatedEvent("/srv/app/Job.py.tmp.1.a"),
        FileModifiedEvent("/srv/app/Job.py.tmp.1.a"),
        FileMovedEvent("/srv/app/Job.py.tmp.1.a", "/srv/app/Job.py"),
        FileDeletedEvent("/srv/app/Job.py"),
    ):
        handler.dispatch(event)

    assert triggers == ["reload"]
