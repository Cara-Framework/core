"""schedule:work stops on SIGTERM after its running inline job, exiting 0.

``docker stop`` sends SIGTERM. Python's default disposition killed the
scheduler at once (exit 143 in 0 s, image5 2026-11-10) while the inline jobs
(``VerifySyncTasksJob``, ``PollAmazonNotificationsJob``) were mid-run in its
thread pool. The scheduler runs in a child process here, because the bug IS
the process dying: an in-process SIGTERM without the fix would kill pytest.
"""

from __future__ import annotations

import signal
import subprocess
import sys
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

_SCHEDULER = textwrap.dedent(
    """
    import importlib
    import os
    import signal
    import sys
    import threading
    import time
    from types import SimpleNamespace

    module = importlib.import_module("cara.commands.core.ScheduleWorkCommand")
    ScheduleWorkCommand = module.ScheduleWorkCommand

    class InlineJobDriver:
        # One inline job that is mid-run when the stop signal arrives.
        def __init__(self):
            self.scheduler = SimpleNamespace(running=True)
            self.waited = None
            self._job = None

        def start(self):
            def _job():
                os.kill(os.getpid(), signal.SIGTERM)
                time.sleep(0.5)
                print("inline job finished", flush=True)

            self._job = threading.Thread(target=_job)
            self._job.start()

        def shutdown(self, wait=True):
            self.waited = wait
            if wait:
                self._job.join()
            self.scheduler.running = False

    driver = InlineJobDriver()
    module.Schedule = SimpleNamespace(driver=lambda _name: driver)

    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command.shutdown_requested = False
    command._reload_requested = False
    command._show_scheduler_status = lambda: None
    command._publish_schedule_snapshot = lambda _driver: None

    before = signal.getsignal(signal.SIGTERM)
    command._start_scheduler({"driver_name": "fake", "run_once": False})
    print(f"waited={driver.waited}", flush=True)
    print(f"restored={signal.getsignal(signal.SIGTERM) is before}", flush=True)
    """
)


def test_sigterm_finishes_the_running_inline_job_then_exits_cleanly() -> None:
    done = subprocess.run(
        [sys.executable, "-c", _SCHEDULER],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert done.returncode != -signal.SIGTERM, "SIGTERM killed the scheduler outright"
    assert done.returncode == 0, done.stderr
    lines = done.stdout.splitlines()
    assert lines[-3:] == ["inline job finished", "waited=True", "restored=True"]
