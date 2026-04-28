"""
Scheduling Provider for the Cara framework.

This module provides the service provider that configures and registers the scheduling subsystem,
making scheduling services available throughout the application.
"""

from typing import Any, Dict, List
from cara.foundation import DeferredProvider
from cara.configuration import config
from cara.scheduling import Scheduling
from cara.scheduling.drivers import APSchedulerDriver
from cara.exceptions import (
    SchedulingConfigurationException,
    DriverLibraryNotFoundException,
    SchedulingException,
)


class SchedulingProvider(DeferredProvider):
    """
    Deferred provider for the scheduling subsystem.

    Reads configuration and registers the Scheduling manager and its drivers.
    """

    @classmethod
    def provides(cls) -> List[str]:
        return ["scheduling"]

    def register(self) -> None:
        default_driver = config("scheduling.default", None)
        drivers_cfg: Dict[str, Any] = config("scheduling.drivers", {}) or {}

        if not default_driver or default_driver not in drivers_cfg:
            raise SchedulingConfigurationException(
                "Missing or invalid 'scheduling.default' or 'scheduling.drivers' config."
            )

        manager = Scheduling(self.application, default_driver)

        self._add_apscheduler_driver(manager)

        self.application.bind("scheduling", manager)

    def boot(self) -> None:
        # No automatic startup here; starting the scheduler is the responsibility
        # of schedule:work command or application boot logic if desired.
        pass

    def _add_apscheduler_driver(self, manager: Scheduling) -> None:
        """Read APScheduler settings from config and register APSchedulerDriver."""
        settings = config("scheduling.drivers.apscheduler")
        if settings is None:
            return

        try:
            driver = APSchedulerDriver(settings)
        except ImportError as e:
            raise DriverLibraryNotFoundException(
                "APSchedulerDriver selected but 'apscheduler' is not installed."
            ) from e
        except Exception as e:
            raise SchedulingException(
                f"Failed to instantiate APSchedulerDriver: {e}"
            ) from e

        manager.add_driver(driver.driver_name, driver)
