"""``craft serve`` reads everything the server writes, and still lets go.

The burst-speed contract lives in ``test_hunt4_crash_recovery_serve_output_drain``;
these pin what the same loop has to keep doing at the edges: the output of a
server that has just died, and a server that is gone while something it
spawned still holds the pipe.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

from cara.commands.core.ServeCommand import ServeCommand

serve_module = importlib.import_module("cara.commands.core.ServeCommand")


def _fake_self(**overrides) -> SimpleNamespace:
    return SimpleNamespace(
        log_colors=SimpleNamespace(colorize_line=lambda line: line),
        console=SimpleNamespace(print=lambda *a, **k: None),
        _terminate_server_tree=lambda proc: True,
        **overrides,
    )


def _spawn(source: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-u", "-c", source],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )


def _reap(child: subprocess.Popen) -> None:
    child.kill()
    child.wait(timeout=5)
    child.stdout.close()


def test_everything_a_crashing_server_wrote_is_printed(capsys):
    # Measured before the fix: 11 of these 200 lines, the traceback never.
    child = _spawn(
        "import sys\n"
        "for i in range(200):\n    print('line', i)\n"
        "raise SystemExit('the reason it crashed')"
    )
    try:
        ServeCommand._monitor_server_process(_fake_self(), child, {})
    finally:
        _reap(child)
    printed = capsys.readouterr().out.splitlines()
    assert len([line for line in printed if line.startswith("line ")]) == 200
    assert "the reason it crashed" in printed


def test_output_still_in_the_pipe_after_the_server_exited_is_read(capsys):
    # The monitor sees the exit after one small read: what is left in the pipe
    # is exactly the end of the output, where the traceback sits.
    child = _spawn("for i in range(40):\n    print('line', i)\n")
    child.wait(timeout=10)
    try:
        with patch.object(serve_module, "_OUTPUT_READ_BYTES", 16):
            ServeCommand._monitor_server_process(_fake_self(), child, {})
    finally:
        _reap(child)
    printed = capsys.readouterr().out.splitlines()
    assert printed == [f"line {i}" for i in range(40)]


def test_a_final_line_without_a_newline_is_not_lost(capsys):
    child = _spawn("import sys; sys.stdout.write('no newline at the end')")
    try:
        ServeCommand._monitor_server_process(_fake_self(), child, {})
    finally:
        _reap(child)
    assert capsys.readouterr().out.splitlines() == ["no newline at the end"]


def test_bytes_that_are_not_utf8_do_not_stop_the_reader(capsys):
    child = _spawn(
        "import sys\n"
        "sys.stdout.buffer.write(b'caf\\xe9 bad\\nafter\\n')\n"
        "sys.stdout.buffer.flush()"
    )
    try:
        ServeCommand._monitor_server_process(_fake_self(), child, {})
    finally:
        _reap(child)
    assert capsys.readouterr().out.splitlines()[-1] == "after"


def test_the_monitor_lets_go_when_a_grandchild_outlives_the_server():
    # uvicorn gone, a reload or worker child still holding stdout: EOF never
    # comes, and the group sweep in ``_start_server`` has to be reached.
    child = _spawn(
        "import subprocess, sys\n"
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
    )
    started = time.monotonic()
    try:
        with patch.object(serve_module, "_EXIT_DRAIN_SECONDS", 0.3):
            ServeCommand._monitor_server_process(_fake_self(), child, {})
        elapsed = time.monotonic() - started
    finally:
        ServeCommand._signal_server_group(child.pid, serve_module.signal.SIGKILL)
        _reap(child)
    assert elapsed < 10
    assert child.returncode is not None


def test_ctrl_c_still_tears_the_server_tree_down():
    child = _spawn("import time; print('up', flush=True); time.sleep(30)")
    torn_down: list[subprocess.Popen] = []

    def _terminate(process: subprocess.Popen) -> bool:
        torn_down.append(process)
        process.kill()
        return True

    fake_self = SimpleNamespace(
        log_colors=SimpleNamespace(colorize_line=lambda line: line),
        console=SimpleNamespace(print=lambda *a, **k: None),
        _terminate_server_tree=_terminate,
    )
    real_select = serve_module.select.select
    calls = {"n": 0}

    def _interrupt_on_second(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] > 1:
            raise KeyboardInterrupt
        return real_select(*args, **kwargs)

    try:
        with patch.object(serve_module.select, "select", _interrupt_on_second):
            ServeCommand._monitor_server_process(fake_self, child, {})
    finally:
        _reap(child)
    assert torn_down == [child]
