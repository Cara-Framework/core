"""Regression tests for ``ServeCommand._start_server`` subprocess cleanup.

Background
~~~~~~~~~~
``_start_server`` spawns the dev server via ``subprocess.Popen`` and
hands the process to ``_monitor_server_process``. The monitor's only
explicit cleanup branch is ``KeyboardInterrupt`` (ctrl-c). If the monitor
raises *any other* exception — e.g. ``colorize_line`` blowing up on a
malformed unicode line, a broken pipe on readline, or any code change
that introduces a new failure path — control jumps to the outer
``except FileNotFoundError`` / ``except Exception`` blocks which just
print an error message and return.

The Popen child is never terminated. The result: orphaned uvicorn
processes pinned to the configured port, blocking the next
``python craft serve`` until they're hand-killed.

The fix wraps the monitor call in a finally block that terminates
(then kills as a fallback) the process if it's still running and
closes the captured stdout pipe.
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from cara.commands.core.ServeCommand import ServeCommand


def _make_serve_command() -> ServeCommand:
    """Build a ServeCommand without firing ``@command`` registration
    side-effects or needing the full application container.

    We patch out the console and the route lister (both touch
    framework state we don't care about for this test)."""
    cmd = ServeCommand.__new__(ServeCommand)
    cmd.console = MagicMock(name="console")
    cmd.log_colors = MagicMock(name="log_colors")
    cmd.application = MagicMock(name="application")
    # ``self.error`` is provided by CommandBase; stub it out so we
    # don't pull in the whole CLI framework.
    cmd.error = MagicMock(name="error")
    return cmd


def _make_fake_popen(still_running: bool = True) -> MagicMock:
    process = MagicMock(spec=subprocess.Popen)
    process.poll.return_value = None if still_running else 0
    process.stdout = MagicMock(name="stdout")
    return process


def test_start_server_terminates_process_when_monitor_raises():
    """The bug: a non-KeyboardInterrupt exception in
    ``_monitor_server_process`` leaks the child uvicorn process. After
    the fix, the process is terminated regardless of how the monitor
    exits."""
    cmd = _make_serve_command()
    fake_process = _make_fake_popen(still_running=True)

    with (
        patch(
            "cara.commands.core.ServeCommand.subprocess.Popen",
            return_value=fake_process,
        ),
        patch.object(
            cmd,
            "_monitor_server_process",
            side_effect=RuntimeError("colorize blew up on bad utf-8"),
        ),
        patch.object(cmd, "_build_server_command", return_value=["/bin/true"]),
    ):
        cmd._start_server({"host": "127.0.0.1", "port": 8000, "reload": False})

    (
        fake_process.terminate.assert_called(),
        (
            "Popen child must be terminated when the monitor raises an "
            "unexpected exception — otherwise orphan uvicorn pinned to the "
            "configured port"
        ),
    )


def test_start_server_kills_process_when_terminate_times_out():
    """``process.terminate()`` is asynchronous; ``wait(timeout=...)``
    bounds how long we'll politely wait. On timeout we must escalate
    to ``kill()`` — otherwise a stuck child still holds the port."""
    cmd = _make_serve_command()
    fake_process = _make_fake_popen(still_running=True)
    fake_process.wait.side_effect = subprocess.TimeoutExpired(cmd="server", timeout=5)

    with (
        patch(
            "cara.commands.core.ServeCommand.subprocess.Popen",
            return_value=fake_process,
        ),
        patch.object(
            cmd,
            "_monitor_server_process",
            side_effect=RuntimeError("boom"),
        ),
        patch.object(cmd, "_build_server_command", return_value=["/bin/true"]),
    ):
        cmd._start_server({"host": "127.0.0.1", "port": 8000, "reload": False})

    fake_process.terminate.assert_called()
    (
        fake_process.kill.assert_called(),
        (
            "If terminate's grace period elapses, escalate to kill — "
            "otherwise a hung child keeps the port bound"
        ),
    )


def test_start_server_closes_stdout_pipe_when_monitor_raises():
    """The PIPE handed to subprocess captures all server stdout; the
    file descriptor must be released even when monitoring blows up,
    or repeated ``serve`` invocations slowly eat fds."""
    cmd = _make_serve_command()
    fake_process = _make_fake_popen(still_running=True)

    with (
        patch(
            "cara.commands.core.ServeCommand.subprocess.Popen",
            return_value=fake_process,
        ),
        patch.object(cmd, "_monitor_server_process", side_effect=RuntimeError("boom")),
        patch.object(cmd, "_build_server_command", return_value=["/bin/true"]),
    ):
        cmd._start_server({"host": "127.0.0.1", "port": 8000, "reload": False})

    (
        fake_process.stdout.close.assert_called(),
        ("captured stdout PIPE must be closed when the monitor exits abnormally"),
    )


def test_start_server_does_not_terminate_already_exited_process():
    """Don't call terminate() on a process that already exited
    naturally (poll() returns an exit code). Avoids spurious 'no such
    process' errors in clean-shutdown paths."""
    cmd = _make_serve_command()
    fake_process = _make_fake_popen(still_running=False)  # already exited

    with (
        patch(
            "cara.commands.core.ServeCommand.subprocess.Popen",
            return_value=fake_process,
        ),
        patch.object(cmd, "_monitor_server_process", return_value=None),
        patch.object(cmd, "_build_server_command", return_value=["/bin/true"]),
    ):
        cmd._start_server({"host": "127.0.0.1", "port": 8000, "reload": False})

    fake_process.terminate.assert_not_called()
    fake_process.kill.assert_not_called()
