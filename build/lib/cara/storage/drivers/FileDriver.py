"""
File-based Storage Driver for the Cara framework.

This module implements a storage driver that saves and retrieves binary data as files in a specified
directory.
"""

import os

from cara.exceptions import (
    KeyNotFoundException,
    StorageConfigurationException,
    StorageException,
)
from cara.storage.contracts import Storage


class FileDriver(Storage):
    """
    File-based Storage Driver for the Cara framework.

    This module implements a storage driver that saves and retrieves binary data as files in a
    specified directory.

    The base directory is injected via constructor (from StorageProvider).
    """

    driver_name = "file"

    def __init__(self, base_directory: str):
        if not base_directory or not isinstance(base_directory, str):
            raise StorageConfigurationException(
                "'storage.drivers.file.path' must be a non-empty string."
            )

        self.base_dir = os.path.abspath(base_directory)
        try:
            os.makedirs(self.base_dir, exist_ok=True)
        except Exception as e:
            raise StorageConfigurationException(
                f"Cannot create storage directory '{self.base_dir}': {e}"
            )

    def put(self, key: str, data: bytes) -> None:
        file_path = self._file_path(key)
        try:
            with open(file_path, "wb") as f:
                f.write(data)
        except Exception as e:
            raise StorageException(f"Failed to write data for key '{key}': {e}")

    def get(self, key: str) -> bytes:
        file_path = self._file_path(key)
        if not os.path.exists(file_path):
            raise KeyNotFoundException(f"Key '{key}' not found in storage.")
        try:
            with open(file_path, "rb") as f:
                return f.read()
        except Exception as e:
            raise StorageException(f"Failed to read data for key '{key}': {e}")

    def delete(self, key: str) -> bool:
        file_path = self._file_path(key)
        if not os.path.exists(file_path):
            return False
        try:
            os.remove(file_path)
            return True
        except Exception as e:
            raise StorageException(f"Failed to delete key '{key}': {e}")

    def exists(self, key: str) -> bool:
        return os.path.exists(self._file_path(key))

    def _file_path(self, key: str) -> str:
        sanitized = key.replace("/", "_").replace("\\", "_")
        return os.path.join(self.base_dir, f"{sanitized}.bin")
