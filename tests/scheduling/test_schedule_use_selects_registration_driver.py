"""``Schedule.use(name)`` / ``call(driver=)`` choose where jobs register.

Live defect in ``cara/scheduling/Scheduling.py``: ``schedule:work --driver X``
ran driver X, but every ``Schedule.call()`` registered on the DEFAULT
driver, so X started with nothing on it. ``use(name)`` now selects the
driver later registrations go to (refusing an unregistered name at once,
not at start-up), and ``call(..., driver=)`` registers on a named one.
``SchedulingContract`` now declares the ``last_run_failures`` /
``last_run_failed_ids`` report that ``schedule:work --once`` reads.
"""

from __future__ import annotations

import pytest

from cara.exceptions import DriverNotRegisteredException
from cara.scheduling.contracts.SchedulingContract import SchedulingContract
from cara.scheduling.Scheduling import Scheduling


class _Driver(SchedulingContract):
    def __init__(self) -> None:
        self.jobs: list[str] = []

    def schedule_job(self, identifier, callback, spec, options=None):
        self.jobs.append(identifier)


def _manager() -> tuple[Scheduling, _Driver, _Driver]:
    default, other = _Driver(), _Driver()
    manager = Scheduling(None, "default")
    manager.add_driver("default", default)
    manager.add_driver("other", other)
    return manager, default, other


def test_use_sends_later_registrations_to_the_chosen_driver() -> None:
    manager, default, other = _manager()

    manager.use("other")
    manager.call(lambda: None).interval(seconds=5)

    assert len(other.jobs) == 1
    assert default.jobs == []
    assert manager.driver() is other


def test_use_none_returns_to_the_configured_default() -> None:
    manager, default, _other = _manager()

    manager.use("other").use(None)
    manager.call(lambda: None).interval(seconds=5)

    assert len(default.jobs) == 1


def test_use_refuses_an_unregistered_name_immediately() -> None:
    manager, _default, _other = _manager()

    with pytest.raises(DriverNotRegisteredException):
        manager.use("missing")
    assert manager.driver() is _default, "a refused use() changes nothing"


def test_call_registers_on_a_named_driver() -> None:
    manager, default, other = _manager()

    manager.call(lambda: None, driver="other").interval(seconds=5)

    assert len(other.jobs) == 1
    assert default.jobs == []


def test_call_on_an_unregistered_driver_is_refused() -> None:
    manager, _default, _other = _manager()

    with pytest.raises(DriverNotRegisteredException):
        manager.call(lambda: None, driver="missing")


def test_contract_declares_the_last_run_failure_report() -> None:
    driver = _Driver()

    assert driver.last_run_failures == 0
    assert list(driver.last_run_failed_ids) == []
