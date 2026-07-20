from __future__ import annotations

import os
import subprocess
import sys


def test_scheduler_module_does_not_import_http_request_stack() -> None:
    script = """
import sys
from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand

assert ScheduleWorkCommand.__name__ == "ScheduleWorkCommand"
assert "cara.http.request.Request" not in sys.modules
assert "multipart" not in sys.modules
assert "python_multipart" not in sys.modules
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in [os.getcwd(), env.get("PYTHONPATH", "")] if part
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=False,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
