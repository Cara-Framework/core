"""One pool of PostgreSQL sessions per configured connection.

A pool is the idle sessions, the permits and the check-in times of ONE
connection: its configured name plus the DSN and session options it actually
connects with (``PostgresConnection._pool``). Every connection name used to
share one module-level list and one semaphore, so two configured connections
— another database, another role, another ``search_path`` — were handed each
other's sessions: a statement meant for the reporting replica ran on a
session of the primary, under the primary's role, and one connection's
traffic spent the other's permits.

The registry is read under ``_REGISTRY_LOCK`` and each pool guards its own
state, so a slow warm-up or a busy pool never stalls another connection.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
import weakref
from collections.abc import Callable, Hashable
from typing import Any

_logger = logging.getLogger("cara.database.pool")

_REGISTRY_LOCK = threading.Lock()
_POOLS: dict[Hashable, PostgresPool] = {}


class PostgresPool:
    """The idle sessions and permits of one configured connection."""

    def __init__(self, label: str, max_size: int) -> None:
        #: What metrics and logs call this pool. Never the DSN: it carries
        #: the password.
        self.label = label
        self.max_size = max_size
        self._lock = threading.Lock()
        self._idle: list[Any] = []
        self._permits = threading.Semaphore(max_size)
        self._in_use = 0
        # ``time.monotonic()`` of each idle session's check-in. Weak keys: an
        # entry dies with its session. No entry (pre-warmed, or a driver
        # object that cannot be weakly referenced) means "probe before use".
        self._idle_since: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
        self._warm_lock = threading.Lock()
        self._warmed = False

    def warm(self, connect: Callable[[], Any], count: int) -> None:
        """Open ``count`` sessions ahead of the first checkout, once.

        Callers that arrive while the first one is still connecting wait for
        it, as they did when one lock covered the whole initialisation — but
        only callers of THIS pool.
        """
        if self._warmed:
            return
        with self._warm_lock:
            if self._warmed:
                return
            for _ in range(count or 0):
                try:
                    session = connect()
                    session.autocommit = True
                except Exception as warm_err:
                    # A cold database at boot leaves the pool with fewer
                    # pre-warmed sessions; lazy connect recovers later. Logged,
                    # or a boot against a dead database is invisible.
                    _logger.warning(
                        "Pool warm-up connect failed for %s (%s pre-warmed); "
                        "falling back to lazy connect: %s",
                        self.label,
                        len(self._idle),
                        warm_err,
                    )
                    break
                with self._lock:
                    self._idle.append(session)
            self._warmed = True

    def acquire(self, timeout: float) -> bool:
        """Take one permit, waiting up to ``timeout`` seconds."""
        if not self._permits.acquire(timeout=timeout):
            return False
        with self._lock:
            self._in_use += 1
        return True

    def release(self) -> None:
        """Hand one permit back. The caller releases each permit once."""
        with self._lock:
            self._in_use -= 1
        self._permits.release()

    def take(self) -> tuple[Any, float | None]:
        """The most recently parked session and when it was parked, or
        ``(None, None)``. The caller holds a permit of this pool."""
        with self._lock:
            if not self._idle:
                return None, None
            session = self._idle.pop()
            idle_since = None
            with contextlib.suppress(TypeError):
                idle_since = self._idle_since.pop(session, None)
            return session, idle_since

    def give_back(self, session: Any) -> None:
        """Park an open session for the next checkout, or close it.

        The reset (a ``ROLLBACK`` round trip when a transaction was left
        open) runs before the pool's lock is taken: it used to run inside the
        one process-wide lock, so every checkout and check-in of every
        connection waited on that round trip.
        """
        try:
            reusable = not session.closed
            if reusable:
                if session.info.transaction_status != 0:
                    session.rollback()
                session.autocommit = True
        except Exception as reset_err:
            # A session that cannot be reset is not handed to the next caller.
            _logger.debug("Pooled session reset failed; closing it: %s", reset_err)
            reusable = False
        if reusable:
            with self._lock:
                if len(self._idle) < self.max_size:
                    self._idle.append(session)
                    with contextlib.suppress(TypeError):
                        self._idle_since[session] = time.monotonic()
                    return
        with contextlib.suppress(Exception):
            session.close()

    def stats(self) -> dict[str, Any]:
        """Saturation of this pool: permits out, idle sessions, the ceiling."""
        with self._lock:
            return {
                "connection": self.label,
                "in_use": self._in_use,
                "idle": len(self._idle),
                "max": self.max_size,
            }

    def drain(self, *, close: bool = True) -> None:
        """Let go of every idle session, closing each unless ``close`` is
        False. Permits already out stay with their holders."""
        with self._lock:
            idle, self._idle = self._idle, []
        if not close:
            return
        for session in idle:
            with contextlib.suppress(Exception):
                session.close()

    @classmethod
    def of(cls, key: Hashable, label: str, max_size: int) -> PostgresPool:
        """The pool registered under ``key``, created on first use.

        The first caller's ``max_size`` is the pool's: a key is one configured
        connection, whose ceiling does not change within a process.
        """
        with _REGISTRY_LOCK:
            pool = _POOLS.get(key)
            if pool is None:
                pool = _POOLS[key] = cls(label, max_size)
            return pool

    @classmethod
    def all_stats(cls) -> list[dict[str, Any]]:
        """``stats()`` of every pool this process has opened (the metrics
        sampler's source: one entry per configured connection)."""
        with _REGISTRY_LOCK:
            pools = list(_POOLS.values())
        return [pool.stats() for pool in pools]

    @classmethod
    def reset_all(cls, *, close: bool = True) -> None:
        """Forget every pool and close its idle sessions (process shutdown,
        test isolation). A wrapper still holding a permit hands it back to
        its own, now unregistered, pool.

        ``close=False`` is for a forked child: the idle sessions it inherited
        are the PARENT's sockets, and closing one here sends the server a
        terminate on the parent's session. psycopg2 spares a session born in
        another process only when it is garbage-collected, not when it is
        closed, so the child drops its references and opens its own.
        """
        with _REGISTRY_LOCK:
            pools = list(_POOLS.values())
            _POOLS.clear()
        for pool in pools:
            pool.drain(close=close)


def database_pool_stats() -> dict[str, int] | None:
    """Saturation of the database pools of this process, summed over every
    configured connection: ``{"in_use", "idle", "max"}`` — permits out, idle
    sessions, the ceilings together. None until a first pool exists.

    The one door for readers outside the ORM (the metrics sampler): it used
    to read this module's globals by name, which one pool per connection no
    longer has. ``PostgresPool.all_stats()`` is the per-connection view.
    """
    pools = PostgresPool.all_stats()
    if not pools:
        return None
    return {
        figure: sum(pool[figure] for pool in pools)
        for figure in ("in_use", "idle", "max")
    }


__all__ = ["PostgresPool", "database_pool_stats"]
