"""
Process-wide ``PUBLISH`` client for ``RedisBroadcaster``.

Why a synchronous client
------------------------
Queue workers run every job under its own ``asyncio.run``, and an asyncio
Redis client can neither outlive nor be shared across event loops. The
broadcaster used to pool one async client per loop and never closed a pool
when its loop ended, so every job that broadcast left one ESTABLISHED socket
behind until the process hit its open-file ceiling (Errno 24, "Too many open
files"). One synchronous client serves every loop instead; the wire work runs
on this publisher's own thread, off the event loop thread.

Why one writer
--------------
Frames used to go out on sixteen threads, one round trip each. Two frames
published back to back raced each other to the wire and reached the other
nodes reversed about half the time (measured 48 %): "paid" before "created".
Every frame now joins one FIFO, and ONE thread drains it. The order other
nodes see is the order ``publish`` was called in this process, whichever
thread or event loop called it.

Why a pipeline
--------------
Whatever is waiting when the writer comes round goes out in one pipeline, one
round trip for the lot, so a burst costs the writer no more round trips than
it has to. A frame on its own is a plain ``PUBLISH``.
"""

from __future__ import annotations

import asyncio
import contextlib
import queue
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

try:
    import redis as redis_sync
except ImportError:
    redis_sync = None  # type: ignore[assignment]

from cara.exceptions import BroadcastingConfigurationException

# The writer's socket, and one spare for a caller that uses ``client()``.
_MAX_CONNECTIONS = 2

# Frames per pipeline. A burst past this goes out as several round trips, so
# one reply never has to carry an unbounded batch.
_MAX_BATCH = 512


class RedisPublisher:
    """One lazily built sync Redis client and one writer per broadcaster."""

    def __init__(self, redis_url: str, connection_config: dict[str, Any]) -> None:
        if redis_sync is None:
            raise BroadcastingConfigurationException(
                "redis is required for RedisBroadcaster. Install with: pip install redis"
            )
        self._redis_url = redis_url
        self._connection_config = connection_config
        self._client: Any | None = None
        self._pool: Any | None = None
        # ``client()`` is public, so the first calls can still race here.
        self._lock = threading.Lock()
        # Frames waiting for the writer, in the order ``publish`` took them.
        self._outbox: queue.SimpleQueue[tuple[str, str, Future[None]]] = (
            queue.SimpleQueue()
        )
        # ONE worker: it is the single writer. An executor rather than a bare
        # thread because the interpreter joins executor threads on exit, so a
        # frame already taken is still sent when the process stops; the thread
        # starts only when the first frame needs it.
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="cara-broadcast-publish"
        )

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
        """PUBLISH in call order, off the event loop thread.

        The frame takes its place in the outbox BEFORE the first await: the
        place is the ordering guarantee, and a caller that is cancelled while
        it waits has still published. What is awaited is only the outcome.

        Not the loop's default executor: ``asyncio.run`` joins that one before
        it returns, so a publish stuck on a slow Redis would hold a finished
        job for a full socket timeout, past the drain's bound and into the
        worker's hard-kill window.
        """
        sent: Future[None] = Future()
        self._outbox.put((channel, payload, sent))
        # One drain per frame is the simple way to never strand a frame: a
        # drain that finds the outbox already emptied by an earlier one
        # returns at once.
        self._executor.submit(self._drain)
        await asyncio.wrap_future(sent)

    def _drain(self) -> None:
        """Send everything waiting, oldest first. Runs on the writer thread."""
        while True:
            frames: list[tuple[str, str]] = []
            waiting: list[Future[None]] = []
            while len(frames) < _MAX_BATCH:
                try:
                    channel, payload, sent = self._outbox.get_nowait()
                except queue.Empty:
                    break
                # A caller that was cancelled before its turn has still
                # published: the frame goes out, there is just nobody left to
                # tell. Marking the future running also pins it, so a cancel
                # that lands mid-send cannot make settling it below raise and
                # strand the rest of the batch.
                frames.append((channel, payload))
                if sent.set_running_or_notify_cancel():
                    waiting.append(sent)
            if not frames:
                return
            try:
                self._send(frames)
            except Exception as error:
                # Not retried: a batch that failed mid-flight may have reached
                # Redis in part, and a second send would deliver those frames
                # twice. Every waiting caller learns its frame may be lost.
                for sent in waiting:
                    sent.set_exception(error)
            else:
                for sent in waiting:
                    sent.set_result(None)

    def _send(self, frames: list[tuple[str, str]]) -> None:
        client = self.client()
        if len(frames) == 1:
            client.publish(*frames[0])
            return
        # Not a transaction: MULTI/EXEC buys nothing for independent frames,
        # and a pipeline already writes them in order on one connection.
        pipeline = client.pipeline(transaction=False)
        for channel, payload in frames:
            pipeline.publish(channel, payload)
        pipeline.execute()

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
            # A blocking pool: past its size a caller waits up to five seconds
            # for a socket, then fails like any other Redis error, where a
            # plain pool would answer "Too many connections" at once.
            "max_connections": _MAX_CONNECTIONS,
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
