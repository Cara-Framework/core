"""Authenticated, non-executable file cache driver.

Values use the same canonical tagged-JSON codec as Redis. The payload is
authenticated with the independent cache signing key, size/depth bounded,
and decoded without importing classes or invoking object hooks. Malformed
or tampered values are deleted and reported as integrity failures unless a
caller explicitly marks the cache as disposable acceleration.
"""

from __future__ import annotations

import contextlib
import glob
import hashlib
import logging
import math
import os
import re
import threading
import time
from collections.abc import Iterable
from typing import Any

from cara.cache.codecs import JsonCacheCodec
from cara.cache.contracts import CacheContract
from cara.exceptions import CacheConfigurationException, ConfigurationException
from cara.facades import Log
from cara.support import ProcessFileLock

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
# Hex digits of the key hash appended to a filename that does not spell its
# key exactly (the key was sanitized or truncated).
_DIGEST_HEX = 32
# Counters (``increment``) live in their own file namespace, as Redis keeps
# them under its ``c:`` prefix beside the ``v:`` values: ``get(key)`` never
# sees a counter and ``increment(key)`` never sees a ``put`` value. ``=`` is
# outside ``_UNSAFE_KEY_CHARS``'s whitelist, so no value filename carries it.
_COUNTER_SUFFIX = "=c.cache"
_VALUE_SUFFIX = ".cache"

_MAX_FILE_BYTES = (
    len(JsonCacheCodec.MAGIC)
    + JsonCacheCodec.TAG_BYTES
    + JsonCacheCodec.MAX_PAYLOAD_BYTES
)


class FileCacheDriver(CacheContract):
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
        self._default_ttl = self._resolve_ttl(None, default_ttl)
        self._codec = JsonCacheCodec(signing_key)
        self._validate_directory(cache_directory)
        requested_directory = os.path.abspath(cache_directory)
        os.makedirs(requested_directory, exist_ok=True)
        self.cache_directory = os.path.realpath(requested_directory)
        self._process_lock_path = os.path.join(self.cache_directory, ".cara-cache.lock")
        self._thread_lock = threading.RLock()

    def _validate_directory(self, directory: str) -> None:
        if not directory or not isinstance(directory, str):
            raise CacheConfigurationException(
                "`cache.drivers.file.path` must be a non‐empty string."
            )

    def get(self, key: str, default: Any = None, *, strict: bool = True) -> Any:
        with self._exclusive():
            return self._get_unlocked(key, default, strict=strict)

    def many(
        self, keys: Iterable[str], default: Any = None, *, strict: bool = True
    ) -> dict[str, Any]:
        """Read several keys under ONE lock acquisition — a consistent
        snapshot, and one process file lock instead of one per key."""
        with self._exclusive():
            return {key: self._get_unlocked(key, default, strict=strict) for key in keys}

    def _get_unlocked(self, key: str, default: Any = None, *, strict: bool = True) -> Any:
        return self._read_live(self._file_path(key), key, default, strict=strict)

    def _read_live(
        self, file_path: str, key: str, default: Any = None, *, strict: bool = True
    ) -> Any:
        """The unexpired value stored at ``file_path``, else ``default``."""
        if not os.path.exists(file_path):
            return default

        ok, expires_at, stored_value = self._read_file(file_path)
        if not ok:
            if strict:
                raise CacheConfigurationException(
                    f"Unreadable cache value for key '{key}'"
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
        strict: bool = True,
    ) -> None:
        expires_at = self._compute_expiration(ttl)
        file_path = self._file_path(key)
        with self._exclusive():
            self._write_file(file_path, expires_at, value, strict=strict)

    def forever(self, key: str, value: Any) -> None:
        file_path = self._file_path(key)
        with self._exclusive():
            self._write_file(file_path, None, value, strict=True)

    def forget(self, key: str) -> bool:
        """Delete the value AND the counter under ``key`` (Redis ``forget``
        deletes both namespaces too)."""
        file_path = self._file_path(key)
        counter_path = self._counter_path(key)
        with self._exclusive():
            deleted_value = self._delete_file(file_path)
            deleted_counter = self._delete_file(counter_path)
            return deleted_value or deleted_counter

    def pull(self, key: str, default: Any = None) -> Any:
        """Atomically return and delete a file-backed cache entry.

        The read and delete are serialized across threads and processes.
        """
        file_path = self._file_path(key)
        with self._exclusive():
            if not os.path.exists(file_path):
                return default
            ok, expires_at, stored_value = self._read_file(file_path)
            if not ok:
                raise CacheConfigurationException(
                    f"Unreadable one-time cache value for key '{key}'"
                )
            if expires_at is not None and expires_at < time.time():
                self._delete_file(file_path)
                return default
            if not self._delete_file(file_path):
                return default
            return stored_value

    def flush(self) -> None:
        with self._exclusive():
            for filename in os.listdir(self.cache_directory):
                if filename.endswith(".cache"):
                    full_path = os.path.join(self.cache_directory, filename)
                    self._delete_file(full_path)

    def has(self, key: str) -> bool:
        """Check if a value or a counter exists under ``key`` (Redis checks
        both namespaces)."""
        with self._exclusive():
            return self._has_unlocked(self._file_path(key), key) or self._has_unlocked(
                self._counter_path(key), key
            )

    def _has_unlocked(self, file_path: str, key: str) -> bool:
        if not os.path.exists(file_path):
            return False
        ok, expires_at, _ = self._read_file(file_path)
        if not ok:
            raise CacheConfigurationException(f"Unreadable cache value for key '{key}'")
        if expires_at is None or expires_at >= time.time():
            return True

        self._delete_file(file_path)
        return False

    def add(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> bool:
        """Atomically add a value only if the key doesn't exist.

        A process file lock serializes expiry inspection and publication.
        ``O_CREAT|O_EXCL`` remains a second kernel-level no-overwrite fence.
        """
        file_path = self._file_path(key)
        expires_at = self._compute_expiration(ttl)
        try:
            payload = self._codec.encode((expires_at, value))
        except CacheConfigurationException as exc:
            raise CacheConfigurationException(
                f"Cannot encode flight-claim value for key '{key}': {exc}"
            ) from exc

        with self._exclusive():
            if os.path.exists(file_path):
                ok, existing_exp, _ = self._read_file(file_path)
                if not ok:
                    raise CacheConfigurationException(
                        f"Unreadable flight-claim value for key '{key}'"
                    )
                if existing_exp is None or existing_exp >= time.time():
                    return False
                self._delete_file(file_path)

            try:
                fd = os.open(
                    file_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                with os.fdopen(fd, "wb") as f:
                    f.write(payload)
                return True
            except Exception as e:
                Log.warning("[FileCacheDriver] add write failed: %s", e, category="cache")
                with contextlib.suppress(OSError):
                    os.remove(file_path)
                raise

    # --- Private Helper Methods ---

    @contextlib.contextmanager
    def _exclusive(self):
        """Serialize compound file-cache operations across all workers."""
        with (
            self._thread_lock,
            ProcessFileLock(self._process_lock_path, timeout_seconds=5),
        ):
            yield

    def _counter_path(self, key: str) -> str:
        """Where ``increment`` keeps the counter for ``key``."""
        return self._file_path(key, suffix=_COUNTER_SUFFIX)

    def _file_path(self, key: str, *, suffix: str = _VALUE_SUFFIX) -> str:
        prefixed_key = f"{self._prefix}{key}"
        # Whitelist sanitize — replacing only "/" was insufficient. A key
        # like "../etc/passwd" with the previous implementation became
        # ".._etc_passwd" (safe), but ``..\\..`` on Windows or ``\x00``
        # NUL injection would still escape on non-POSIX layers. Strict
        # whitelist closes the class entirely.
        sanitized = _UNSAFE_KEY_CHARS.sub("_", prefixed_key)
        if sanitized != prefixed_key or len(sanitized) > _MAX_FILENAME_LEN:
            # Long keys get truncated + hashed so collisions stay
            # vanishingly improbable while filenames remain bounded. A key the
            # sanitizer CHANGED is hashed too: replacing every unsafe character
            # with ``_`` mapped ``search:q=red shoes`` and ``search:q=red_shoes``
            # (or ``a/b`` and ``a_b``) to one file, so one key was served the
            # other's value and a lock on one refused the other.
            digest = hashlib.sha256(prefixed_key.encode("utf-8")).hexdigest()[
                :_DIGEST_HEX
            ]
            sanitized = f"{sanitized[: _MAX_FILENAME_LEN - _DIGEST_HEX - 1]}_{digest}"
        candidate = os.path.join(self.cache_directory, f"{sanitized}{suffix}")
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
        ttl_seconds = self._resolve_ttl(ttl, self._default_ttl)
        return None if ttl_seconds == 0 else time.time() + ttl_seconds

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
        except CacheConfigurationException, TypeError, ValueError:
            self._delete_file(file_path)
            return False, None, None

    def _write_file(
        self,
        file_path: str,
        expires_at: float | None,
        value: Any,
        *,
        strict: bool = True,
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
            raise

    def increment(self, key: str, amount: int = 1, ttl: int | None = None) -> int:
        """Increment a counter, serialized across local worker processes.

        Compound counter updates are serialized between threads and processes.
        Invalid input or stored state raises instead of resetting an authority
        counter to a weaker value. The counter is kept in its own namespace
        (``_COUNTER_SUFFIX``), as Redis keeps it: ``get(key)`` afterwards
        misses, exactly as it does on the production driver, instead of
        answering an int no Redis deployment would ever return.
        """
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise TypeError("Cache counter amount must be an integer")
        if ttl is None:
            raise CacheConfigurationException(
                "Cache counters require a positive expiration"
            )
        ttl = self._resolve_ttl(ttl, self._default_ttl)
        if ttl == 0:
            raise CacheConfigurationException(
                "Cache counters require a positive expiration"
            )
        counter_path = self._counter_path(key)
        with self._exclusive():
            sentinel = object()
            current = self._read_live(counter_path, key, sentinel, strict=True)
            is_new = current is sentinel
            if not is_new and (isinstance(current, bool) or not isinstance(current, int)):
                raise CacheConfigurationException(
                    f"Cache counter '{key}' contains a non-integer value"
                )
            new_val = (0 if is_new else current) + amount
            # Redis INCRBY semantics: ``ttl`` applies on creation (or to
            # a key that lost its expiry) — re-passing it on every hit
            # reset the expiry window, so a fixed-window counter under
            # sustained traffic never expired.
            if is_new:
                effective_ttl = ttl
            else:
                remaining = self._ttl_at(counter_path, key)
                effective_ttl = remaining if remaining is not None else ttl
            expires_at = self._compute_expiration(effective_ttl)
            self._write_file(counter_path, expires_at, new_val, strict=True)
            return new_val

    def throttle(
        self,
        key: str,
        *,
        emission_interval_ms: int,
        burst: int,
        cost: int = 1,
    ) -> tuple[bool, int, int, int]:
        """GCRA cell under the process file lock — see ``CacheContract.throttle``."""
        for name, value in (
            ("emission_interval_ms", emission_interval_ms),
            ("burst", burst),
            ("cost", cost),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise CacheConfigurationException(
                    f"Cache throttle {name} must be a positive integer"
                )
        # The cell lives in the COUNTER namespace, as Redis keeps it (an
        # INCRBY-style integer under the counter prefix): a ``put`` / ``get``
        # of the same key never reads or overwrites the bucket, and
        # ``forget`` / ``ttl`` see it exactly as they do on Redis.
        cell_path = self._counter_path(key)
        with self._exclusive():
            now = int(time.time() * 1000)
            sentinel = object()
            stored = self._read_live(cell_path, key, sentinel, strict=True)
            if stored is not sentinel and (
                isinstance(stored, bool) or not isinstance(stored, int)
            ):
                raise CacheConfigurationException(
                    f"Cache throttle cell '{key}' contains a non-integer value"
                )
            tolerance = emission_interval_ms * burst
            tat = now if stored is sentinel or stored < now else stored
            next_tat = tat + emission_interval_ms * cost
            allow_at = next_tat - tolerance
            if now < allow_at:
                remaining = max(0, (tolerance - (tat - now)) // emission_interval_ms)
                return False, remaining, allow_at - now, tat - now
            full_after = next_tat - now
            self._write_file(
                cell_path,
                self._compute_expiration(math.ceil(full_after / 1000)),
                next_tat,
                strict=True,
            )
            remaining = max(0, (tolerance - full_after) // emission_interval_ms)
            return True, remaining, 0, full_after

    def forget_if(self, key: str, expected_value: Any) -> bool:
        """
        Re-read the entry, compare against ``expected_value``, and delete on
        match under the driver CAS lock.
        """
        with self._exclusive():
            file_path = self._file_path(key)
            if not os.path.exists(file_path):
                return False
            ok, expires_at, stored_value = self._read_file(file_path)
            if not ok:
                raise CacheConfigurationException(
                    f"Unreadable compare-and-delete value for key '{key}'"
                )
            if expires_at is not None and expires_at < time.time():
                self._delete_file(file_path)
                return False
            if stored_value != expected_value:
                return False
            return self._delete_file(file_path)

    def touch_if(self, key: str, expected_value: Any, ttl: int) -> bool:
        """
        Re-read the entry, compare against ``expected_value``, and rewrite it
        with a fresh expiry on match under the driver CAS lock.
        """
        if isinstance(ttl, bool) or not isinstance(ttl, int) or ttl <= 0:
            raise CacheConfigurationException(
                "touch_if requires a positive integer ttl in seconds"
            )
        with self._exclusive():
            file_path = self._file_path(key)
            if not os.path.exists(file_path):
                return False
            ok, expires_at, stored_value = self._read_file(file_path)
            if not ok:
                raise CacheConfigurationException(
                    f"Unreadable compare-and-touch value for key '{key}'"
                )
            if expires_at is not None and expires_at < time.time():
                self._delete_file(file_path)
                return False
            if stored_value != expected_value:
                return False
            self._write_file(file_path, time.time() + ttl, stored_value, strict=True)
            return True

    def ttl(self, key: str) -> int | None:
        """Remaining time-to-live for ``key`` in seconds.

        Mirrors ``RedisCacheDriver.ttl``: the counter's expiry when a
        counter exists under ``key``, else the value's; ``None`` when
        neither exists or it has no expiry, otherwise the integer seconds
        until expiration. Used by the throttle middleware to report an
        accurate ``Retry-After`` header instead of the full window.
        """
        with self._exclusive():
            counter_path = self._counter_path(key)
            if os.path.exists(counter_path):
                return self._ttl_at(counter_path, key)
            return self._ttl_at(self._file_path(key), key)

    def _ttl_at(self, file_path: str, key: str) -> int | None:
        if not os.path.exists(file_path):
            return None
        ok, expires_at, _ = self._read_file(file_path)
        if not ok:
            raise CacheConfigurationException(f"Unreadable cache value for key '{key}'")
        if expires_at is None:
            return None
        remaining = expires_at - time.time()
        return max(1, int(remaining)) if remaining > 0 else None

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

        # A key the sanitizer changed is stored as ``<name>_<hash>``; a
        # pattern that does not end in ``*`` must still reach it.
        hashed_pattern = os.path.join(
            self.cache_directory,
            f"{sanitized_pattern}_{'[0-9a-f]' * _DIGEST_HEX}.cache",
        )
        # Counters too, as the Redis driver's one ``[vc]`` walk deletes both
        # namespaces. (A pattern ending in ``*`` already reaches them.)
        counter_patterns = [
            os.path.join(self.cache_directory, f"{sanitized_pattern}{_COUNTER_SUFFIX}"),
            os.path.join(
                self.cache_directory,
                f"{sanitized_pattern}_{'[0-9a-f]' * _DIGEST_HEX}{_COUNTER_SUFFIX}",
            ),
        ]

        deleted_count = 0
        with self._exclusive():
            matching_files = {
                *glob.glob(file_pattern),
                *glob.glob(hashed_pattern),
                *(path for pattern in counter_patterns for path in glob.glob(pattern)),
            }
            for file_path in matching_files:
                if self._delete_file(file_path):
                    deleted_count += 1

        return deleted_count
