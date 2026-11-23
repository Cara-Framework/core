"""
File-based Storage Driver for the Cara framework.

This module implements a storage driver that saves and retrieves binary data as files in a specified
directory.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import stat
import uuid
from pathlib import Path, PurePosixPath

from cara.exceptions import (
    KeyNotFoundException,
    StorageConfigurationException,
    StorageException,
)
from cara.storage.contracts import StorageContract


class FileDriver(StorageContract):
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
        """Store ``data`` under ``key``; the key holds the old bytes or the
        new ones, never a mix.

        Written to a sibling temp file and renamed over the key. Opening the
        key itself with ``"wb"`` truncated it first: a reader during an
        overwrite got an empty or half-written object (a scrubbed photo read
        back mid-rewrite and cached upstream), and a write that failed
        left the key empty instead of unchanged.
        """
        file_path = self._file_path(key)
        directory = os.path.dirname(file_path)
        tmp_path = os.path.join(directory, f".tmp-{os.getpid()}-{uuid.uuid4().hex}")
        try:
            fd = self._create_in(directory, tmp_path)
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            # An overwrite keeps the permissions the key already had.
            with contextlib.suppress(OSError):
                os.chmod(tmp_path, stat.S_IMODE(os.stat(file_path).st_mode))
            os.replace(tmp_path, file_path)
        except Exception as e:
            with contextlib.suppress(OSError):
                os.remove(tmp_path)
            raise StorageException(f"Failed to write data for key '{key}': {e}") from e

    @staticmethod
    def _create_in(directory: str, path: str) -> int:
        """Create ``path`` (new, default permissions) inside ``directory``.

        ``delete`` prunes a directory the moment it is empty, so a delete of
        the last sibling can remove ``directory`` between the ``makedirs`` and
        the create here — an upload racing the worker that discards the
        previous import. Recreate it and try again instead of failing the put.
        """
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        for _ in range(2):
            os.makedirs(directory, exist_ok=True)
            with contextlib.suppress(FileNotFoundError):
                return os.open(path, flags, 0o666)
        os.makedirs(directory, exist_ok=True)
        return os.open(path, flags, 0o666)

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
