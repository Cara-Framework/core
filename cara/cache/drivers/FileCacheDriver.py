"""
File-based Cache Driver for the Cara framework.

This module implements a cache driver that stores cache entries as files on disk,
using pickle serialization and handling expiration logic.

Stores each cache entry as a pickle file in `cache_directory`.
Filenames are formed as: prefix + sanitized_key + ".cache".
Expired entries are removed on access.
"""

from cara.facades import Log
import glob
import hashlib
import os
import pickle
import re
import threading
import time
from typing import Any, Optional

from cara.cache.contracts import Cache
from cara.exceptions import CacheConfigurationException

# Anything outside this whitelist gets replaced before being used in a
# filename. Keeping ``:`` (Cara cache key separator) and ``-`` / ``.``
# preserves human-readable cache files while making path traversal
# (``..``, ``/``, ``\\``, NUL) impossible at the filename layer.
_UNSAFE_KEY_CHARS = re.compile(r"[^A-Za-z0-9._:\-]")
# Same as above but also permits glob metacharacters so ``forget_pattern``
# can still build wildcard expressions ("home:*", "products:?").
_UNSAFE_PATTERN_CHARS = re.compile(r"[^A-Za-z0-9._:\-\*\?\[\]]")
# Filename length cap (most filesystems error around 255 bytes; reserve
# room for the ``.cache`` suffix and any hash suffix we append).
_MAX_FILENAME_LEN = 200

# Process-wide lock used by ``forget_if`` to make the CAS sequence safe
# against concurrent in-process releases. Cross-process racing is out of
# scope for the file driver — distributed locks belong on Redis.
_FILE_CAS_LOCK = threading.Lock()


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

        ok, expires_at, stored_value = self._read_file(file_path)
        if not ok:
            # Read/unpickle failed — treat as cache miss so remember()'s sentinel
            # logic kicks in. Previously we returned `stored_value` (None) which
            # caused Cache.remember to return None even when the caller's
            # callback produced a real value, breaking tuple-unpacking sites.
            return default

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

        ok, expires_at, _ = self._read_file(file_path)
        if not ok:
            return False
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

    def remember(
        self,
        key: str,
        ttl: int,
        callback,
    ) -> Any:
        """
        Get value from cache or execute callback and cache the result.

        If the key exists and hasn't expired, return the cached value.
        Otherwise, execute the callback, cache its result, and return it.
        """
        _MISSING = object()
        cached = self.get(key, _MISSING)
        if cached is not _MISSING:
            return cached

        value = callback()
        self.put(key, value, ttl)
        return value

    # --- Private Helper Methods ---

    def _file_path(self, key: str) -> str:
        prefixed_key = f"{self._prefix}{key}"
        # Whitelist sanitize — replacing only "/" was insufficient. A key
        # like "../etc/passwd" with the previous implementation became
        # ".._etc_passwd" (safe), but ``..\\..`` on Windows or ``\x00``
        # NUL injection would still escape on non-POSIX layers. Strict
        # whitelist closes the class entirely.
        sanitized = _UNSAFE_KEY_CHARS.sub("_", prefixed_key)
        if len(sanitized) > _MAX_FILENAME_LEN:
            # Long keys get truncated + hashed so collisions stay
            # vanishingly improbable while filenames remain bounded.
            digest = hashlib.sha256(prefixed_key.encode("utf-8")).hexdigest()[:32]
            sanitized = f"{sanitized[: _MAX_FILENAME_LEN - 33]}_{digest}"
        candidate = os.path.join(self.cache_directory, f"{sanitized}.cache")
        # Defense in depth: reject any resolved path that escapes the
        # cache directory. ``realpath`` collapses symlinks too, so a
        # cache_directory containing a symlinked subdir can't be abused
        # to land writes outside the configured root.
        resolved = os.path.realpath(candidate)
        root_with_sep = self.cache_directory.rstrip(os.sep) + os.sep
        if not (resolved == self.cache_directory or resolved.startswith(root_with_sep)):
            raise ValueError(
                "FileCacheDriver: refusing to operate on cache file outside the cache root"
            )
        return candidate

    def _compute_expiration(self, ttl: Optional[int]) -> Optional[float]:
        if ttl is not None:
            return None if ttl <= 0 else time.time() + ttl
        return time.time() + self._default_ttl

    def _read_file(self, file_path: str) -> tuple[bool, Optional[float], Any]:
        try:
            with open(file_path, "rb") as f:
                expires_at, value = pickle.load(f)
                return True, expires_at, value
        except Exception:
            return False, None, None

    def _write_file(
        self,
        file_path: str,
        expires_at: Optional[float],
        value: Any,
    ) -> None:
        try:
            with open(file_path, "wb") as f:
                pickle.dump((expires_at, value), f)
        except Exception as e:
            Log.debug(f"[FileCacheDriver] write failed: {e}", category="cache")

    def _delete_file(self, file_path: str) -> bool:
        try:
            os.remove(file_path)
            return True
        except FileNotFoundError:
            return False
        except Exception:
            return False

    def forget_if(self, key: str, expected_value: Any) -> bool:
        """
        Best-effort CAS on a file-backed cache: re-read the entry, compare
        against ``expected_value``, and delete on match. The whole sequence
        runs under a per-process lock so concurrent ``release()`` calls in
        the same worker can't both succeed; cross-process racing is still
        possible (file caches are not the right primitive for distributed
        locks — use Redis), but at least every individual worker's
        local view stays consistent.
        """
        with _FILE_CAS_LOCK:
            file_path = self._file_path(key)
            if not os.path.exists(file_path):
                return False
            ok, expires_at, stored_value = self._read_file(file_path)
            if not ok:
                return False
            if expires_at is not None and expires_at < time.time():
                self._delete_file(file_path)
                return False
            if stored_value != expected_value:
                return False
            return self._delete_file(file_path)

    def forget_pattern(self, pattern: str) -> int:
        """
        Delete multiple cache files matching a glob pattern.

        Converts cache key pattern to file glob pattern and deletes matching files.

        Args:
            pattern: Glob pattern (e.g., "home:*", "products:featured:*")

        Returns:
            Number of files deleted
        """
        # Convert cache key pattern to file path pattern. Same whitelist
        # sanitize as ``_file_path`` but with glob metacharacters allowed
        # so wildcard invalidation still works.
        prefixed_pattern = f"{self._prefix}{pattern}"
        sanitized_pattern = _UNSAFE_PATTERN_CHARS.sub("_", prefixed_pattern)
        file_pattern = os.path.join(self.cache_directory, f"{sanitized_pattern}.cache")

        deleted_count = 0
        try:
            matching_files = glob.glob(file_pattern)
            for file_path in matching_files:
                if self._delete_file(file_path):
                    deleted_count += 1
        except Exception as e:
            Log.debug(f"[FileCacheDriver] forget_pattern failed: {e}", category="cache")

        return deleted_count
