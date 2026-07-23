"""Authenticated, non-executable file cache driver.

Values use the same canonical tagged-JSON codec as Redis. The payload is
authenticated with the independent cache signing key, size/depth bounded,
and decoded without importing classes or invoking object hooks. Legacy
pickle files and malformed/tampered values are deleted as cache misses.
"""

from __future__ import annotations

import contextlib
import glob
import hashlib
import logging
import os
import re
import threading
import time
from typing import Any

from cara.cache.codecs import JsonCacheCodec
from cara.cache.contracts import Cache
from cara.exceptions import CacheConfigurationException, ConfigurationException
from cara.facades import Log

_logger = logging.getLogger("cara.cache.file")

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

_MAX_FILE_BYTES = (
    len(JsonCacheCodec.MAGIC)
    + JsonCacheCodec.TAG_BYTES
    + JsonCacheCodec.MAX_PAYLOAD_BYTES
)


class FileCacheDriver(Cache):
    """
    File-based Cache Driver for the Cara framework.

    Stores authenticated tagged-JSON cache entries in `cache_directory`.
    Filenames are formed as: prefix + sanitized_key + ".cache".
    Expired entries are removed on access.
    """

    driver_name = "file"

    def __init__(
        self,
        cache_directory: str,
        prefix: str = "",
        default_ttl: int = 60,
        *,
        signing_key: str | bytes,
    ):
        self._prefix = prefix or ""
        self._default_ttl = default_ttl
        self._codec = JsonCacheCodec(signing_key)
        self._validate_directory(cache_directory)
        self.cache_directory = os.path.abspath(cache_directory)
        os.makedirs(self.cache_directory, exist_ok=True)

    def _validate_directory(self, directory: str) -> None:
        if not directory or not isinstance(directory, str):
            raise CacheConfigurationException(
                "`cache.drivers.file.path` must be a non‐empty string."
            )

    def get(self, key: str, default: Any = None, *, strict: bool = False) -> Any:
        file_path = self._file_path(key)
        if not os.path.exists(file_path):
            return default

        ok, expires_at, stored_value = self._read_file(file_path)
        if not ok:
            # Decode failed — treat as cache miss so remember()'s sentinel
            # logic kicks in. Previously we returned `stored_value` (None) which
            # caused Cache.remember to return None even when the caller's
            # callback produced a real value, breaking tuple-unpacking sites.
            if strict:
                raise CacheConfigurationException(
                    f"Unreadable cache value for security-sensitive key '{key}'"
                )
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
        ttl: int | None = None,
        *,
        strict: bool = False,
    ) -> None:
        expires_at = self._compute_expiration(ttl)
        file_path = self._file_path(key)
        self._write_file(file_path, expires_at, value, strict=strict)

    def forever(self, key: str, value: Any) -> None:
        file_path = self._file_path(key)
        self._write_file(file_path, None, value)

    def forget(self, key: str) -> bool:
        file_path = self._file_path(key)
        return self._delete_file(file_path)

    def pull(self, key: str, default: Any = None) -> Any:
        """Atomically return and delete a file-backed cache entry.

        The file driver only promises in-process atomicity; distributed
        one-time claims must use Redis.
        """
        file_path = self._file_path(key)
        with _FILE_CAS_LOCK:
            if not os.path.exists(file_path):
                return default
            ok, expires_at, stored_value = self._read_file(file_path)
            if not ok:
                return default
            if expires_at is not None and expires_at < time.time():
                self._delete_file(file_path)
                return default
            if not self._delete_file(file_path):
                return default
            return stored_value

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
        ttl: int | None = None,
    ) -> bool:
        """Atomically add a value only if the key doesn't exist.

        ``has()`` then ``put()`` was a TOCTOU window: two callers
        could both observe the missing key and both ``put`` it. This
        broke the flight-claim pattern (``Cache.add(flight_key, True,
        ttl)``) — both workers thought they'd claimed the slot and
        both ran the work. The fix uses ``O_CREAT|O_EXCL`` so the
        kernel performs the existence-check + create atomically; the
        loser of the race sees ``FileExistsError``.

        Stale (expired) files are still claimable: if the file exists
        but has expired, we delete it under the CAS lock and retry
        the exclusive open.
        """
        file_path = self._file_path(key)
        expires_at = self._compute_expiration(ttl)

        for _ in range(2):  # at most one expired-file retry
            try:
                # ``os.open`` returns an OS-level fd; wrap with the
                # exclusive flag so a concurrent caller racing with us
                # gets FileExistsError.
                fd = os.open(
                    file_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
            except FileExistsError:
                # Existing entry — but it might be expired. Promote
                # to a CAS-locked check + delete under the same lock
                # the rest of the file-CAS code uses, so we don't
                # race with concurrent forget_if.
                with _FILE_CAS_LOCK:
                    if not os.path.exists(file_path):
                        # Vanished between the two checks — retry.
                        continue
                    ok, existing_exp, _ = self._read_file(file_path)
                    if ok and (existing_exp is None or existing_exp >= time.time()):
                        return False
                    # Expired or unreadable — drop it and retry.
                    self._delete_file(file_path)
                continue

            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(self._codec.encode((expires_at, value)))
                return True
            except Exception as e:
                Log.warning("[FileCacheDriver] add write failed: %s", e, category="cache")
                # Roll back the empty/partial file we created.
                with contextlib.suppress(OSError):
                    os.remove(file_path)
                return False

        return False

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
        _missing = object()
        cached = self.get(key, _missing)
        if cached is not _missing:
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
            raise ConfigurationException(
                "FileCacheDriver: refusing to operate on cache file outside the cache root"
            )
        return candidate

    def _compute_expiration(self, ttl: int | None) -> float | None:
        if ttl is not None:
            return None if ttl <= 0 else time.time() + ttl
        return time.time() + self._default_ttl

    def _read_file(self, file_path: str) -> tuple[bool, float | None, Any]:
        """Read one bounded, authenticated, non-executable cache envelope."""
        try:
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(file_path, flags)
            with os.fdopen(fd, "rb") as f:
                blob = f.read(_MAX_FILE_BYTES + 1)
        except Exception:
            return False, None, None

        if len(blob) > _MAX_FILE_BYTES:
            self._delete_file(file_path)
            return False, None, None

        try:
            decoded = self._codec.decode(blob)
            if not isinstance(decoded, tuple) or len(decoded) != 2:
                raise CacheConfigurationException(
                    "File cache envelope must contain expiry and value."
                )
            expires_at, value = decoded
            if expires_at is not None and not isinstance(expires_at, (int, float)):
                raise CacheConfigurationException(
                    "File cache expiry must be numeric or null."
                )
            return True, expires_at, value
        except (CacheConfigurationException, TypeError, ValueError):
            self._delete_file(file_path)
            return False, None, None

    def _write_file(
        self,
        file_path: str,
        expires_at: float | None,
        value: Any,
        *,
        strict: bool = False,
    ) -> None:
        """Atomically write one authenticated tagged-JSON envelope."""
        tmp_path = f"{file_path}.tmp.{os.getpid()}.{int(time.time() * 1000)}"
        try:
            payload = self._codec.encode((expires_at, value))
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(tmp_path, flags, 0o600)
            with os.fdopen(fd, "wb") as f:
                f.write(payload)
            os.replace(tmp_path, file_path)
        except Exception as e:
            Log.warning("[FileCacheDriver] write failed: %s", e, category="cache")
            # Best-effort cleanup of the tmp file.
            with contextlib.suppress(OSError):
                os.remove(tmp_path)
            if strict:
                raise

    def _delete_file(self, file_path: str) -> bool:
        try:
            os.remove(file_path)
            return True
        except FileNotFoundError:
            return False
        except Exception:
            _logger.warning("cache file deletion failed", exc_info=True)
            return False

    def increment(self, key: str, amount: int = 1, ttl: int | None = None) -> int:
        """Increment a counter, serialised within the process.

        Cross-process incrementing on a file-backed cache is
        fundamentally unsafe — distributed counters need Redis. But
        within a single Python process (multi-thread / async),
        wrapping the read-modify-write in ``_FILE_CAS_LOCK`` at
        least prevents lost updates between coroutines on the same
        worker. Previously this was a plain non-atomic RMW.
        """
        with _FILE_CAS_LOCK:
            sentinel = object()
            current = self.get(key, sentinel)
            is_new = current is sentinel
            try:
                new_val = (0 if is_new else int(current)) + amount
            except (TypeError, ValueError):
                is_new = True
                new_val = amount
            # Redis INCRBY semantics: ``ttl`` applies on creation (or to
            # a key that lost its expiry) — re-passing it on every hit
            # reset the expiry window, so a fixed-window counter under
            # sustained traffic never expired.
            if is_new:
                effective_ttl = ttl
            else:
                remaining = self.ttl(key)
                effective_ttl = remaining if remaining is not None else ttl
            self.put(key, new_val, effective_ttl)
            return new_val

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

    def ttl(self, key: str) -> int | None:
        """Remaining time-to-live for ``key`` in seconds.

        Mirrors ``RedisCacheDriver.ttl``: returns ``None`` when the
        file is missing or has no expiry, otherwise the integer
        seconds until expiration. Used by the throttle middleware to
        report an accurate ``Retry-After`` header instead of the full
        window.
        """
        file_path = self._file_path(key)
        ok, expires_at, _ = self._read_file(file_path)
        if not ok or expires_at is None:
            return None
        remaining = int(expires_at - time.time())
        return remaining if remaining > 0 else None

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
            Log.warning(
                "[FileCacheDriver] forget_pattern failed: %s", e, category="cache"
            )

        return deleted_count
