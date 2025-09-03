"""
Storage Provider for the Cara framework.

This module provides the deferred service provider that configures and registers the storage
subsystem, including file-based storage drivers.
"""

from typing import List

from cara.configuration import config
from cara.exceptions import StorageConfigurationException
from cara.foundation import DeferredProvider
from cara.storage import Storage
from cara.storage.drivers import FileDriver
from cara.support import paths


class StorageProvider(DeferredProvider):
    """
    Deferred provider for the storage subsystem.

    Reads configuration and registers the Storage manager and its drivers.
    """

    @classmethod
    def provides(cls) -> List[str]:
        return ["storage"]

    def register(self) -> None:
        """Register storage services with configuration."""
        settings = config("storage", {})
        default_driver = settings.get("default", "file")

        storage_manager = Storage(self.application, default_driver)

        # Register storage drivers
        self._add_file_driver(storage_manager, settings)

        self.application.bind("storage", storage_manager)

    def _add_file_driver(self, storage_manager: Storage, settings: dict) -> None:
        """Register file storage driver with configuration."""
        file_settings = settings.get("drivers", {}).get("file", None)
        if not file_settings:
            raise StorageConfigurationException(
                "Missing or invalid 'storage.drivers.file' config."
            )

        raw_path = file_settings.get("path")
        if not isinstance(raw_path, str) or not raw_path:
            raise StorageConfigurationException(
                "'storage.drivers.file.path' must be a non-empty string."
            )

        full_path = paths("base", raw_path)
        driver = FileDriver(base_directory=full_path)
        storage_manager.add_driver(FileDriver.driver_name, driver)
