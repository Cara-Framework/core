"""
File-based Cache Driver for the Cara framework.

This module implements a cache driver that stores cache entries as files on disk,
using pickle serialization and handling expiration logic.

Stores each cache entry as a pickle file in `cache_directory`.
Filenames are formed as: prefix + sanitized_key + ".cache".
Expired entries are removed on access.
"""

import os
import pickle
import time
from typing import Any, Optional

from cara.cache.contracts import Cache
from cara.exceptions import CacheConfigurationException


class FileCacheDriver(Cache):
    """
    File-based Cache Driver for the Cara framework.

    This module implements a cache driver that stores cache entries as files on disk,
    using pickle serialization and handling expiration logic.

    Stores each cache entry as a pickle file in `cache_directory`.
    Filenames are formed as: prefix + sanitized_key + ".cache".
    Expired entries are removed on access.
    """

    driver_name = "file"

    def __init__(
        self,
        cache_directory: str,
        prefix: str = "",
        default_ttl: int = 60,
    ):
        self._prefix = prefix or ""
        self._default_ttl = default_ttl
        self._validate_directory(cache_directory)
        self.cache_directory = os.path.abspath(cache_directory)
        os.makedirs(self.cache_directory, exist_ok=True)

    def _validate_directory(self, directory: str) -> None:
        if not directory or not isinstance(directory, str):
            raise CacheConfigurationException(
                "`cache.drivers.file.path` must be a non‐empty string."
            )

    def get(self, key: str, default: Any = None) -> Any:
        file_path = self._file_path(key)
        if not os.path.exists(file_path):
            return default

        expires_at, stored_value = self._read_file(file_path)
        if expires_at is None or expires_at >= time.time():
            return stored_value

        # Entry expired: delete and return default
        self._delete_file(file_path)
        return default

    def put(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> None:
        expires_at = self._compute_expiration(ttl)
        file_path = self._file_path(key)
        self._write_file(file_path, expires_at, value)

    def forever(self, key: str, value: Any) -> None:
        file_path = self._file_path(key)
        self._write_file(file_path, None, value)

    def forget(self, key: str) -> bool:
        file_path = self._file_path(key)
        return self._delete_file(file_path)

    def flush(self) -> None:
        for filename in os.listdir(self.cache_directory):
            if filename.endswith(".cache"):
                full_path = os.path.join(self.cache_directory, filename)
                self._delete_file(full_path)

    def has(self, key: str) -> bool:
        """Check if a key exists in cache."""
        file_path = self._file_path(key)
        if not os.path.exists(file_path):
            return False

        expires_at, _ = self._read_file(file_path)
        if expires_at is None or expires_at >= time.time():
            return True

        # Entry expired: delete and return False
        self._delete_file(file_path)
        return False

    def add(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """Add a value only if key doesn't exist. Returns True if added."""
        if self.has(key):
            return False
        
        self.put(key, value, ttl)
        return True

    # --- Private Helper Methods ---

    def _file_path(self, key: str) -> str:
        prefixed_key = f"{self._prefix}{key}"
        sanitized = prefixed_key.replace("/", "_")
        return os.path.join(self.cache_directory, f"{sanitized}.cache")

    def _compute_expiration(self, ttl: Optional[int]) -> Optional[float]:
        if ttl is not None:
            return None if ttl <= 0 else time.time() + ttl
        return time.time() + self._default_ttl

    def _read_file(self, file_path: str) -> tuple[Optional[float], Any]:
        try:
            with open(file_path, "rb") as f:
                return pickle.load(f)
        except Exception:
            return None, None

    def _write_file(
        self,
        file_path: str,
        expires_at: Optional[float],
        value: Any,
    ) -> None:
        try:
            with open(file_path, "wb") as f:
                pickle.dump((expires_at, value), f)
        except Exception:
            pass

    def _delete_file(self, file_path: str) -> bool:
        try:
            os.remove(file_path)
            return True
        except FileNotFoundError:
            return False
        except Exception:
            return False
