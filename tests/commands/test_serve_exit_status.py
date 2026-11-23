"""``craft serve`` exits with the server's own status.

It is the production API launcher (``exec python craft serve``). ``handle()``
returned None whatever happened, so the craft process exited 0 when the server
child crashed at boot, was killed, or never started (port taken, a
configuration error, a missing executable): a supervisor, a deploy script or
CI read a dead API as a clean stop. ``CommandRunner`` turns an int from
``handle()`` into the exit status; None (0) is now kept for a requested stop.

The server child is a real ``python -c`` subprocess; no port is bound.
"""

from __future__ import annotations

import signal
import sys
from types import SimpleNamespace

import pytest

from cara.commands.core.ServeCommand import ServeCommand
from cara.exceptions import InvalidArgumentException


class _Quiet:
    def __getattr__(self, _name):
        return lambda *_a, **_k: None


class _StopWhenReady:
    """Colours the child's output; a ``ready`` line is the operator's Ctrl+C."""

    def colorize_line(self, line: str) -> str:
        if line == "ready":
            raise KeyboardInterrupt
        return line


def _launcher(argv: list[str], *, port_busy: bool = False) -> ServeCommand:
    command = ServeCommand.__new__(ServeCommand)
    command.console = _Quiet()
    command.errors = []
    command.error = lambda message, *_a, **_k: command.errors.append(message)
    command.log_colors = SimpleNamespace(colorize_line=lambda line: line)
    command._prepare_server_config = lambda *_a: {
        "host": "127.0.0.1",
        "port": 59999,
        "reload": False,
        "workers": 1,
    }
    command._show_server_config = lambda _config: None
    command._show_routes_compact = lambda: None
    command._port_has_listener = lambda *_a: port_busy
    command._build_server_command = lambda _config: argv
    return command


def _child(code: str) -> list[str]:
    return [sys.executable, "-c", code]


@pytest.mark.parametrize(
    "code, status",
    [
        ("import sys; print('boom'); sys.exit(3)", 3),
        ("import os, signal; os.kill(os.getpid(), signal.SIGKILL)", 128 + signal.SIGKILL),
        ("pass", 0),
    ],
    ids=["boot-error", "killed", "clean-exit"],
)
def test_a_server_that_ends_by_itself_reports_its_own_status(code, status):
    assert _launcher(_child(code)).handle() == status


def test_a_taken_port_is_a_failed_start():
    command = _launcher(_child("raise SystemExit(0)"), port_busy=True)

    assert command.handle() == 1
    assert "already has a live listener" in command.errors[0]


def test_a_configuration_error_is_a_failed_start():
    command = _launcher(_child("pass"))

    def _refuse(*_a):
        raise InvalidArgumentException("port must be a number")

    command._prepare_server_config = _refuse

    assert command.handle() == 1


def test_a_missing_server_executable_is_a_failed_start(tmp_path):
    command = _launcher([str(tmp_path / "no-such-uvicorn")])

    assert command.handle() == 1
    assert "executable not found" in command.errors[0]


def test_a_launcher_fault_is_a_failed_start():
    command = _launcher(_child("pass"))

    def _broken(_config):
        raise RuntimeError("cannot build the server command")

    command._build_server_command = _broken

    assert command.handle() == 1


def test_a_requested_stop_is_a_clean_exit():
    command = _launcher(_child("import time; print('ready', flush=True); time.sleep(60)"))
    command.log_colors = _StopWhenReady()

    assert command.handle() is None
