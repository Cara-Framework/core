"""A driver shut down and started again (the schedule:work hot reload)
must fire its jobs — not reuse executors whose thread pools are closed."""

from __future__ import annotations

import threading

import pytest

pytest.importorskip("apscheduler")

from cara.scheduling.drivers.APSchedulerDriver import APSchedulerDriver


def test_restart_after_shutdown_fires_jobs() -> None:
    driver = APSchedulerDriver()
    fired = threading.Event()
    driver.scheduler.start()
    driver.shutdown(wait=True)

    driver.scheduler.add_job(fired.set, trigger="date", id="probe")
    driver.scheduler.start()
    try:
        assert fired.wait(5), "a job never fired after shutdown + start"
    finally:
        driver.shutdown(wait=False)
