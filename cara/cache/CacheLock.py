"""CacheLock."""

from __future__ import annotations

import asyncio
import os
import time
import uuid


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

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Sync context manager exit."""
        self.release()
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
        self.release()
        return False
