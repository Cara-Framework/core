"""
The subscriber side of ``RedisBroadcaster``: one pubsub connection, one owner.

Split out of ``RedisBroadcaster`` for the file budget (DOCTRINE §5) and bound
back onto the class there, so these are its methods in everything but address.

Three rules live here, each bought by a live defect:

* The listener task is the ONLY owner of its pubsub. ``_listener_pubsub`` is
  set only while that pubsub is live (connected and carrying every channel in
  ``_redis_subscribed``); while it is ``None`` a subscription change edits the
  set and nothing else, and the listener applies the set to the connection it
  is building. A SUBSCRIBE sent on the half-built pubsub used to open a second
  connection that nobody read.
* ``_redis_subscribed`` is the wanted state and is always edited first, so
  every (re)connect ends up carrying exactly it.
* The read is a poll with a PING behind it. redis-py's ``listen()`` waits for
  the next frame with no timeout, and a half-open connection never fails that
  wait: the node went deaf until its process restarted.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from cara.facades import Log

# The listener's watchdog. A read comes back empty-handed every poll; after
# ``_PING_AFTER`` seconds without a single byte from Redis the listener sends a
# PING, and ``_PONG_WITHIN`` seconds later with still nothing heard the
# connection is taken for dead. Worst case a half-open connection is noticed in
# about 25 s, where it used to deafen the node until the process restarted.
_POLL_SECONDS = 1.0
_PING_AFTER = 15.0
_PONG_WITHIN = 10.0


async def _ensure_redis_subscription(self, prefixed_channel: str) -> None:
    """Subscribe the Redis listener to ``prefixed_channel`` if not
    already. Lazily starts the listener task on first call."""
    if prefixed_channel in self._redis_subscribed:
        return

    async with self._listener_lock:
        if prefixed_channel in self._redis_subscribed:
            return  # Re-check inside the lock.

        # The set is the desired state, and it is edited first, always:
        # whatever happens to the live connection below, the listener
        # subscribes to all of it every time it (re)connects.
        self._redis_subscribed.add(prefixed_channel)

        # Start the listener task on first subscription. The task
        # subscribes to every channel in ``_redis_subscribed`` so
        # everything we've added so far is picked up.
        if self._listener_task is None or self._listener_task.done():
            self._listener_task = asyncio.create_task(self._listener_loop())
            # Wait for initial subscribe to complete so callers can
            # publish immediately after; bounded so we don't deadlock
            # when Redis is unreachable.
            try:
                await asyncio.wait_for(self._listener_ready.wait(), timeout=5.0)
            except TimeoutError:
                Log.warning(
                    "Redis listener didn't become ready within 5s; "
                    "broadcasts may briefly miss subscribers",
                    category="cara.broadcasting",
                )
            return

        async with self._pubsub_lock:
            # ``None``: the listener is between connections. Subscribing
            # here used to open a SECOND connection on the half-built
            # pubsub; the listener then read only one of the two and every
            # channel on the other went silent until the next restart. The
            # listener picks this channel up from the set instead.
            pubsub = self._listener_pubsub
            if pubsub is None or prefixed_channel not in self._redis_subscribed:
                return
            try:
                await pubsub.subscribe(prefixed_channel)
            except Exception as e:
                Log.warning(
                    "Failed to subscribe Redis listener to %s: %s",
                    prefixed_channel,
                    e,
                    category="cara.broadcasting",
                )
                # Keep the channel in ``_redis_subscribed``: that set
                # is what the listener re-subscribes after it
                # reconnects, and the local subscription (and the
                # client's "subscribed" reply) already stand. Dropping
                # it here meant a tab that subscribed during a Redis
                # outage was never wired once Redis came back — a
                # private per-user channel has no second subscriber
                # to trigger another attempt.
                #
                # And make that reconnect happen: redis-py may have
                # re-established this connection on its own, WITHOUT
                # the channel that just failed, and the listener would
                # read on happily and never learn of it.
                self._listener_resync = True


async def _drop_redis_subscription(self, prefixed_channel: str) -> None:
    if prefixed_channel not in self._redis_subscribed:
        return
    self._redis_subscribed.discard(prefixed_channel)
    async with self._pubsub_lock:
        # Same rule as subscribing: only a live pubsub is touched. Between
        # connections the set alone decides what the next one carries.
        pubsub = self._listener_pubsub
        if pubsub is not None and prefixed_channel not in self._redis_subscribed:
            try:
                await pubsub.unsubscribe(prefixed_channel)
            except Exception as e:
                # Left subscribed on the wire at worst: its frames find no
                # local subscriber, and the next reconnect leaves it out.
                Log.debug(
                    "Redis unsubscribe of %s failed: %s",
                    prefixed_channel,
                    e,
                    category="cara.broadcasting",
                )
    # Optimisation: if we no longer have any subscriptions, the
    # listener can shut down to free a connection.
    if (
        not self._redis_subscribed
        and self._listener_task
        and not self._listener_task.done()
    ):
        Log.info(
            "Redis listener idle (no subscriptions); shutting down",
            category="cara.broadcasting",
        )
        self._listener_task.cancel()
        self._listener_task = None
        self._listener_ready.clear()


async def _listener_loop(self) -> None:
    """Long-running task that drains pubsub messages and dispatches
    them to local subscribers. Auto-reconnects with exponential
    backoff on Redis failures.

    This task is the only owner of its pubsub. Nothing else sees the
    object until ``_subscribe_until_settled`` has put every wanted
    channel on it, and it is withdrawn (``_retire_pubsub``) before the
    backoff sleep, not after: a subscription change that arrived during
    the sleep used to be sent on the dead connection.
    """
    attempt = 0
    while True:
        # A LOCAL, never the instance attribute: ``cleanup()`` and a
        # restarted listener both write ``self._listener_pubsub`` from
        # other tasks, and iterating the attribute once crashed every
        # restart with "'NoneType' object has no attribute 'listen'".
        pubsub: Any = None
        try:
            client = await self._redis()
            if client is None:
                # ``client.pubsub()`` on None would be a confusing crash.
                # Treat as a connection failure -> backoff + retry.
                raise ConnectionError("Redis client unavailable")
            pubsub = client.pubsub()
            await self._subscribe_until_settled(pubsub)
            # No await since the last comparison in there: the pubsub
            # goes live carrying exactly the wanted set.
            self._listener_resync = False
            self._listener_pubsub = pubsub
            self._listener_ready.set()
            attempt = 0  # Reset after a successful (re)connect.

            await self._read_until_silent(pubsub)
        except asyncio.CancelledError:
            await self._retire_pubsub(pubsub)
            Log.debug("Redis listener cancelled", category="cara.broadcasting")
            break
        except Exception as e:
            await self._retire_pubsub(pubsub)
            attempt += 1
            backoff = min(60, 2 ** min(attempt - 1, 6))
            Log.error(
                "Redis listener crashed (attempt %s): %s; retrying in %ss",
                attempt,
                e,
                backoff,
                category="cara.broadcasting",
                exc_info=True,
            )
            await asyncio.sleep(backoff)


async def _subscribe_until_settled(self, pubsub: Any) -> None:
    """Bring a fresh pubsub to exactly ``_redis_subscribed``.

    The set can change during any await here (a tab subscribing while
    Redis is still answering the previous SUBSCRIBE), so this loops until
    a comparison finds nothing left to do.
    """
    applied: set[str] = set()
    while applied != self._redis_subscribed:
        missing = self._redis_subscribed - applied
        extra = applied - self._redis_subscribed
        if missing:
            await pubsub.subscribe(*missing)
        if extra:
            await pubsub.unsubscribe(*extra)
        applied = (applied | missing) - extra


async def _read_until_silent(self, pubsub: Any) -> None:
    """Hand frames to their lanes until the connection stops answering.

    redis-py's ``listen()`` waits for the next frame with no timeout at
    all. On a half-open connection that wait never ends: no error, no
    reconnect, and every cross-process frame for this node is lost until
    the process restarts. So this polls instead, and when Redis has been
    quiet for a while it asks. Anything Redis sends counts as an answer,
    the PONG included; a real frame is the only thing passed on.

    Raises ``ConnectionError`` for a dead connection, which the caller
    treats like any other: retire, back off, reconnect, resubscribe.
    """
    loop = asyncio.get_running_loop()
    last_heard = loop.time()
    pinged_at: float | None = None
    while True:
        if self._listener_resync:
            raise ConnectionError("a SUBSCRIBE failed on the live pub/sub connection")
        message = await pubsub.get_message(timeout=self._poll_seconds)
        now = loop.time()
        if message is not None:
            last_heard, pinged_at = now, None
            if message.get("type") == "message":
                self._enqueue_pubsub_message(message)
        elif pinged_at is not None:
            if now - pinged_at > self._pong_within:
                raise ConnectionError(
                    f"Redis pub/sub connection is silent: no PONG within "
                    f"{self._pong_within:g}s"
                )
        elif now - last_heard > self._ping_after:
            await pubsub.ping()
            pinged_at = now


async def _retire_pubsub(self, pubsub: Any) -> None:
    """Withdraw the listener's pubsub from use, then close it."""
    # Only if it is still ours: a listener cancelled for being idle may
    # get here after its successor has already gone live.
    if pubsub is not None and self._listener_pubsub is pubsub:
        self._listener_pubsub = None
        self._listener_ready.clear()
    if pubsub is None:
        return
    # Under the lock, so a SUBSCRIBE already in flight on it finishes
    # first; closing underneath one made redis-py open a fresh connection
    # for it that nobody would ever read or release.
    async with self._pubsub_lock:
        with contextlib.suppress(OSError, RuntimeError, AttributeError, ConnectionError):
            await pubsub.aclose()
