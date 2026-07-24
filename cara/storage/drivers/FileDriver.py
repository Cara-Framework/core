"""
File-based Storage Driver for the Cara framework.

This module implements a storage driver that saves and retrieves binary data as files in a specified
directory.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path, PurePosixPath

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
            ) from e

    def put(self, key: str, data: bytes) -> None:
        file_path = self._file_path(key)
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(data)
        except Exception as e:
            raise StorageException(f"Failed to write data for key '{key}': {e}") from e

    def get(self, key: str) -> bytes:
        file_path = self._file_path(key)
        if not os.path.exists(file_path):
            raise KeyNotFoundException(f"Key '{key}' not found in storage.")
        try:
            with open(file_path, "rb") as f:
                return f.read()
        except Exception as e:
            raise StorageException(f"Failed to read data for key '{key}': {e}") from e

    def delete(self, key: str) -> bool:
        file_path = self._file_path(key)
        if not os.path.exists(file_path):
            return False
        try:
            os.remove(file_path)
            self._prune_empty_parents(os.path.dirname(file_path))
            return True
        except Exception as e:
            raise StorageException(f"Failed to delete key '{key}': {e}") from e

    def exists(self, key: str) -> bool:
        return os.path.exists(self._file_path(key))

    def delete_directory(self, key: str) -> bool:
        """Delete one logical directory without allowing storage-root escape."""
        directory = self._file_path(key)
        if not os.path.exists(directory):
            return False
        if not os.path.isdir(directory):
            raise StorageException(f"Storage key '{key}' is not a directory.")
        try:
            shutil.rmtree(directory)
            self._prune_empty_parents(os.path.dirname(directory))
            return True
        except Exception as e:
            raise StorageException(
                f"Failed to delete storage directory '{key}': {e}"
            ) from e

    def _file_path(self, key: str) -> str:
        """Resolve a logical key to its exact, hierarchy-preserving path."""
        if not isinstance(key, str) or not key.strip() or "\x00" in key:
            raise StorageException("Storage keys must be non-empty strings.")

        normalized = key.replace("\\", "/")
        logical = PurePosixPath(normalized)
        if logical.is_absolute() or any(part in {".", ".."} for part in logical.parts):
            raise StorageException(f"Unsafe storage key '{key}'.")

        candidate = Path(self.base_dir, *logical.parts).resolve()
        root = Path(self.base_dir).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise StorageException(f"Storage key '{key}' escapes its root.") from exc
        if candidate == root:
            raise StorageException("A storage key cannot address the storage root.")
        return str(candidate)

    def _prune_empty_parents(self, directory: str) -> None:
        root = Path(self.base_dir).resolve()
        current = Path(directory).resolve()
        while current != root:
            try:
                current.relative_to(root)
                current.rmdir()
            except OSError, ValueError:
                return
            current = current.parent
