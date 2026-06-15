"""
Scheduling Core for the Cara framework.

This module implements the core scheduling logic, managing scheduled jobs and their execution.
"""

from __future__ import annotations

import uuid
from typing import Any

from cara.exceptions import DriverNotRegisteredException, InvalidArgumentException
from cara.scheduling import ScheduleBuilder
from cara.scheduling.contracts import Scheduling


class Scheduling:
    """Holds drivers and provides entry for scheduling tasks."""

    def __init__(self, application: Any, default_driver: str):
        self.application = application
        self._default_driver = default_driver
        self._drivers: dict[str, Scheduling] = {}

    def add_driver(self, name: str, driver: Scheduling) -> None:
        self._drivers[name] = driver

    def driver(self, name: str | None = None) -> Scheduling:
        chosen = name or self._default_driver
        inst = self._drivers.get(chosen)
        if not inst:
            raise DriverNotRegisteredException(f"Scheduling driver '{chosen}' not registered.")
        return inst

    def call(self, callback: Any) -> ScheduleBuilder:
        if not callable(callback):
            raise InvalidArgumentException("call requires a callable.")
        unique_id = f"call_{uuid.uuid4().hex}"

        def job_callback():
            callback()

        drv = self.driver()
        return ScheduleBuilder(drv, unique_id, job_callback, options={})
