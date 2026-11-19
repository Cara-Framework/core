"""CacheLock."""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid

_logger = logging.getLogger("cara.cache")


class CacheLock:
    """
    Distributed lock using cache.

    Prevents race conditions in distributed systems using cache as storage.
    """

    def __init__(
        self,
        cache,
        key: str,
        timeout: int = 86400,
        owner: str | None = None,
        *,
        exact_key: bool = False,
    ):
        """
        Initialize a cache lock.

        Owner default: a unique per-instance string. The previous default of
        ``"default"`` collided across processes — process B could ``release``
        process A's lock because both stored the literal string ``"default"``
        as the owner field, and ``forget_if(key, "default")`` matched. Now
        every CacheLock instance gets ``f"{pid}:{uuid}"`` so cross-process
        ownership is unambiguous.

        Args:
            cache: Cache driver instance
            key: Lock key
            timeout: Lock timeout in seconds (default: 24 hours)
            owner: Lock owner identifier (for distinguishing lock holders).
                Pass an explicit value when multiple coroutines / threads
                share the same lock and you want the same owner to be able
                to re-acquire / release. Otherwise leave None for a unique
                per-instance owner.
            exact_key: Keep ``key`` byte-for-byte instead of adding the
                conventional ``lock:`` namespace. Framework callers should
                request this through :meth:`Cache.exact_lock`; ordinary
                :meth:`Cache.lock` names remain prefixed.
        """
        # A lock's TTL must be a real expiry: ``0`` reaches the driver as
        # "no expiry", so a holder that crashed held the key forever.
        if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout < 1:
            raise ValueError("CacheLock timeout must be a positive number of seconds")
        self.cache = cache
        self.key = key if exact_key else f"lock:{key}"
        self.timeout = timeout
        self.owner = owner or f"{os.getpid()}:{uuid.uuid4().hex}"

    # Spin interval between failed acquires. 100ms balances:
    # responsiveness when the lock holder finishes quickly vs. wasted
    # cache hits while waiting. Could be made configurable per-call,
    # but the same number is correct for every site so far.
    _SPIN_INTERVAL_S = 0.1

    def acquire(self, timeout: int = 0) -> bool:
        """
        Attempt to acquire the lock (sync API).

        Args:
            timeout: Max seconds to wait for lock (0 = non-blocking)

        Returns:
            True if lock acquired, False otherwise

        Async callers
        -------------
        ``acquire`` blocks the calling thread on its retry sleep — fine
        from sync code (CLI commands, sync workers), but in an async
        context it would stall the event loop. Async callers should use
        ``await acquire_async(timeout)`` instead, which yields control
        with ``asyncio.sleep`` so other coroutines keep making progress
        while we wait.
        """
        start = time.time()

        while True:
            # Try to add the lock key (only succeeds if key doesn't exist)
            if self.cache.add(self.key, self.owner, self.timeout):
                return True

            if timeout == 0 or (time.time() - start) >= timeout:
                return False

            time.sleep(self._SPIN_INTERVAL_S)

    async def acquire_async(self, timeout: int = 0) -> bool:
        """Async-safe variant of :meth:`acquire`.

        Yields the event loop on each retry interval instead of
        blocking the worker thread. The cache primitive itself
        (``cache.add``) is sync — that's a single fast op so it's
        acceptable to call inline.
        """
        start = time.time()

        while True:
            if self.cache.add(self.key, self.owner, self.timeout):
                return True

            if timeout == 0 or (time.time() - start) >= timeout:
                return False

            await asyncio.sleep(self._SPIN_INTERVAL_S)

    def release(self) -> bool:
        """Release the lock if (and only if) it is still held by this owner.

        Uses ``forget_if`` so the ownership check and the delete happen as
        a single atomic step. The previous "check then delete" pattern had
        a TOCTOU race: between ``get`` and ``forget`` the lock could
        expire and be reacquired by another owner whose key would then be
        wrongly deleted.
        """
        return bool(self.cache.forget_if(self.key, self.owner))

    def __enter__(self):
        """Sync context manager entry — raises if lock cannot be
        acquired within ``self.timeout``.

        NOTE: a 24-hour default block is a footgun when used from an
        async handler (the sync ``acquire`` ``time.sleep``s, freezing
        the event loop). Async callers should use ``async with`` /
        ``acquire_async``.
        """
        if not self.acquire(timeout=self.timeout):
            raise TimeoutError(
                f"Could not acquire lock '{self.key}' within {self.timeout}s"
            )
        return self

    def _release_on_exit(self, body_failed: bool) -> None:
        """Release at the end of a ``with`` block.

        A release that raises while the body's own exception propagates is
        logged, never raised: it REPLACED the body's error (a Paddle failure
        read as a Redis ``forget_if`` error). After a body that succeeded it
        still raises. A release that finds the lock gone (its TTL lapsed
        mid-body, so the section ran unprotected for a while) is logged."""
        try:
            released = self.release()
        except Exception:
            if not body_failed:
                raise
            _logger.warning(
                "lock %s not released while the body failed", self.key, exc_info=True
            )
            return
        if not released:
            _logger.warning(
                "lock %s had lapsed before release; the section outran its TTL (%ss)",
                self.key,
                self.timeout,
            )

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Sync context manager exit."""
        self._release_on_exit(exc_type is not None)
        return False

    async def __aenter__(self):
        """Async context manager entry — uses ``acquire_async`` so the
        event loop keeps running while we wait."""
        if not await self.acquire_async(timeout=self.timeout):
            raise TimeoutError(
                f"Could not acquire lock '{self.key}' within {self.timeout}s"
            )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self._release_on_exit(exc_type is not None)
        return False
