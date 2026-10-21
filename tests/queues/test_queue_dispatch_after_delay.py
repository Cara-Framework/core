"""``Queue.dispatch_after`` must treat its delay as RELATIVE seconds.

It handed the delay to ``schedule(job, when=delay)``, and the AMQP driver
parses ``when`` as an absolute timestamp: ``pendulum.parse("3600")`` is the
year 3600, ``"1800"`` a past year (the job ran at once), ``"300"`` a parse
error. Every delayed queued notification went through that path. The delay
now goes through ``later``, the primitive that already takes seconds or a
``pendulum.Duration``.
"""

from __future__ import annotations

from types import SimpleNamespace

import pendulum
import pytest

from cara.queues.contracts import ShouldQueue
from cara.queues.drivers.AMQPDriver import AMQPDriver
from cara.queues.Queue import Queue


class _Job(ShouldQueue):
    pass


class _RecordingDriver:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def later(self, delay, job, options):
        self.calls.append(("later", delay, job))
        return "job-later"

    def schedule(self, job, when, options):
        self.calls.append(("schedule", when, job))
        return "job-schedule"


def _queue(driver) -> Queue:
    queue = Queue(SimpleNamespace(), "x")
    queue.add_driver("x", driver)
    return queue


def test_dispatch_after_routes_an_instance_through_later() -> None:
    driver = _RecordingDriver()
    job = _Job()

    assert _queue(driver).dispatch_after(job, 300) == "job-later"
    assert driver.calls == [("later", 300, job)]


def test_dispatch_after_routes_a_class_through_later() -> None:
    driver = _RecordingDriver()

    assert _queue(driver).dispatch_after(_Job, 300) == "job-later"
    [(verb, delay, job)] = driver.calls
    assert (verb, delay) == ("later", 300)
    assert isinstance(job, _Job)


class _DelayedStore:
    def __init__(self) -> None:
        self.targets: list[pendulum.DateTime] = []

    def schedule(self, job, target, options):
        self.targets.append(target)
        return "job-delayed"


@pytest.mark.parametrize(
    "delay", [300, 3600, pendulum.duration(seconds=300)], ids=["300", "3600", "duration"]
)
def test_amqp_delayed_row_is_due_delay_seconds_from_now(delay) -> None:
    driver = AMQPDriver.__new__(AMQPDriver)
    driver.options = {"tz": "UTC"}
    driver._delayed_store = _DelayedStore()
    driver.push = lambda *_jobs, options: pytest.fail("a delayed job ran at once")
    expected = delay.total_seconds() if isinstance(delay, pendulum.Duration) else delay

    before = pendulum.now("UTC")
    assert _queue(driver).dispatch_after(_Job(), delay) == "job-delayed"
    after = pendulum.now("UTC")

    [target] = driver._delayed_store.targets
    assert before.add(seconds=expected) <= target <= after.add(seconds=expected)
