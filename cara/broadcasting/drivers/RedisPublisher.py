"""
Process-wide ``PUBLISH`` client for ``RedisBroadcaster``.

Why a synchronous client
------------------------
Queue workers run every job under its own ``asyncio.run``, and an asyncio
Redis client can neither outlive nor be shared across event loops. The
broadcaster used to pool one async client per loop and never closed a pool
when its loop ended, so every job that broadcast left one ESTABLISHED socket
behind until the process hit its open-file ceiling (Errno 24, "Too many open
files"). One synchronous client serves every loop instead; each publish runs
on the default executor, off the event loop thread.

Why a blocking pool
-------------------
Publishes reach the pool from many executor threads at once — a many-channel
``broadcast``, or a busy API process. A non-blocking pool answers that
overflow with "Too many connections", which the broadcaster tolerates as a
Redis blip and logs at debug level: the broadcast is dropped on every other
node and nobody hears about it. This pool waits, bounded, for a free
connection instead.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from typing import Any

try:
    import redis as redis_sync
except ImportError:
    redis_sync = None  # type: ignore[assignment]

from cara.exceptions import BroadcastingConfigurationException


class RedisPublisher:
    """One lazily built, blocking-pool sync Redis client per broadcaster."""

    def __init__(self, redis_url: str, connection_config: dict[str, Any]) -> None:
        if redis_sync is None:
            raise BroadcastingConfigurationException(
                "redis is required for RedisBroadcaster. Install with: pip install redis"
            )
        self._redis_url = redis_url
        self._connection_config = connection_config
        self._client: Any | None = None
        self._pool: Any | None = None
        # The first publishes can race here from several executor threads.
        self._lock = threading.Lock()

    def client(self) -> Any:
        """Return the shared client, building its pool on first use."""
        if self._client is not None:
            return self._client
        with self._lock:
            if self._client is None:
                self._pool = redis_sync.BlockingConnectionPool.from_url(
                    self._redis_url, **self._pool_kwargs()
                )
                self._client = redis_sync.Redis(connection_pool=self._pool)
        return self._client

    async def publish(self, channel: str, payload: str) -> None:
        """PUBLISH off the event loop thread."""
        await asyncio.to_thread(self.client().publish, channel, payload)

    def close(self) -> None:
        """Release every pooled connection; the next publish rebuilds the pool."""
        client, pool = self._client, self._pool
        self._client = None
        self._pool = None
        if client is not None:
            with contextlib.suppress(
                OSError, RuntimeError, AttributeError, ConnectionError
            ):
                client.close()
        if pool is not None:
            # A client built on a caller-supplied pool does not close that pool.
            with contextlib.suppress(
                OSError, RuntimeError, AttributeError, ConnectionError
            ):
                pool.disconnect()

    def _pool_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            # Sixteen sockets at most; a publish waits up to five seconds for
            # one, then fails like any other Redis error.
            "max_connections": 16,
            "timeout": 5,
            "decode_responses": True,
            # Bounded socket timeouts: without them a black-holed Redis stalls
            # a publish for the OS TCP timeout (minutes) instead of failing.
            "socket_connect_timeout": 5,
            "socket_timeout": 5,
            "socket_keepalive": True,
            "health_check_interval": 30,
        }
        if self._redis_url.startswith("rediss://"):
            kwargs.update(
                ssl_ca_certs=self._connection_config.get("ssl_ca_certs") or None,
                ssl_certfile=self._connection_config.get("ssl_certfile") or None,
                ssl_keyfile=self._connection_config.get("ssl_keyfile") or None,
                ssl_cert_reqs=self._connection_config.get("ssl_cert_reqs", "required"),
            )
        return {key: value for key, value in kwargs.items() if value is not None}
