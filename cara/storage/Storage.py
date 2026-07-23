"""
Central Storage Manager for the Cara framework.

This module provides the Storage class, which manages multiple storage drivers and delegates storage
operations to the appropriate driver instance.
"""

from __future__ import annotations

from cara.exceptions import DriverNotRegisteredException


class Storage:
    """
    Central storage manager. Delegates put/get/delete/exists to registered driver instances.

    The default driver name is injected via constructor (from StorageProvider).
    """

    def __init__(self, application, default_driver: str):
        self.application = application
        self._drivers: dict[str, Storage] = {}
        self._default_driver: str = default_driver

    def add_driver(self, name: str, driver: Storage) -> None:
        """Register a driver instance under `name`."""
        self._drivers[name] = driver

    def driver(self, name: str | None = None) -> Storage:
        """
        Return the named driver, or the default if `name` is None.

        Raises DriverNotRegisteredException if not found.
        """
        chosen = name or self._default_driver
        inst = self._drivers.get(chosen)
        if not inst:
            raise DriverNotRegisteredException(
                f"Storage driver '{chosen}' not registered."
            )
        return inst

    def put(
        self,
        key: str,
        data: bytes,
        driver_name: str | None = None,
    ) -> None:
        """Store raw bytes under `key` via selected driver."""
        self.driver(driver_name).put(key, data)

    def get(self, key: str, driver_name: str | None = None) -> bytes:
        """Retrieve bytes for `key` via selected driver."""
        return self.driver(driver_name).get(key)

    def delete(self, key: str, driver_name: str | None = None) -> bool:
        """
        Delete `key` via selected driver.

        Return True if deleted.
        """
        return self.driver(driver_name).delete(key)

    def exists(self, key: str, driver_name: str | None = None) -> bool:
        """Return True if `key` exists in selected driver."""
        return self.driver(driver_name).exists(key)

    def delete_directory(
        self,
        key: str,
        driver_name: str | None = None,
    ) -> bool:
        """Delete one logical directory via the selected driver."""
        return self.driver(driver_name).delete_directory(key)
