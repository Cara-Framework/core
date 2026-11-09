"""``craft serve`` must drain uvicorn's output as fast as uvicorn writes it.

``ServeCommand._monitor_server_process`` reads one line and then sleeps 0.1 s, so
the child's stdout is read at 10 lines a second. Past 64 KB the pipe is full, the
child's logging blocks, and with it every request (loguru's ``enqueue=True`` writes
to a pipe). Measured 2026-11-09, private stack, same API, 8 connections, nothing
wrong with anything: uvicorn alone 68 requests a second, p50 110 ms; through
``craft serve`` (the Dockerfile's CMD) 8 a second, p50 1.9 s. After a Postgres crash
with tracebacks going by, the API did not recover for two minutes after Postgres
came back; uvicorn alone recovered at once.
"""

from __future__ import annotations

import subprocess
import sys
import time
from types import SimpleNamespace

from cara.commands.core.ServeCommand import ServeCommand

_LINES = 200
#: The whole backlog read in about 2 s: at 10 lines a second this takes 20 s.
_DRAIN_BUDGET_SECONDS = 5.0
_CHILD_LINGERS_SECONDS = 1.0


def _monitor(process: subprocess.Popen) -> None:
    fake_self = SimpleNamespace(
        log_colors=SimpleNamespace(colorize_line=lambda line: line),
        console=SimpleNamespace(print=lambda *a, **k: None),
        _terminate_server_tree=lambda proc: True,
    )
    ServeCommand._monitor_server_process(fake_self, process, {})


def test_a_burst_of_server_output_is_read_as_fast_as_it_is_written(capsys):
    # The child stays up a moment after the burst: the monitor only returns on
    # exit, and whatever it has not read by then is lost with the pipe.
    child = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            "import time\n"
            f"for i in range({_LINES}):\n    print('line', i)\n"
            f"time.sleep({_CHILD_LINGERS_SECONDS})",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    started = time.monotonic()
    try:
        _monitor(child)
    finally:
        child.kill()
        child.wait(timeout=5)
        if child.stdout is not None:
            child.stdout.close()
    elapsed = time.monotonic() - started

    printed = capsys.readouterr().out.splitlines()
    assert elapsed < _DRAIN_BUDGET_SECONDS, f"{_LINES} lines took {elapsed:.1f}s to drain"
    assert len(printed) == _LINES


def test_the_monitor_still_returns_when_the_server_exits_silently():
    child = subprocess.Popen(
        [sys.executable, "-c", "pass"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    try:
        _monitor(child)
    finally:
        child.wait(timeout=5)
        if child.stdout is not None:
            child.stdout.close()
    assert child.returncode == 0
