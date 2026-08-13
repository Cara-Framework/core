"""Queue worker runtime collaborator."""

from __future__ import annotations

import asyncio
import logging
import threading

_logger = logging.getLogger("cara.queue.worker")


class ActiveJobCancellationRegistry:
    """Thread-safe registry of async jobs that can be cancelled on shutdown.

    A queue worker owns one registry and shares it with every consumer slot.
    The main thread never touches a consumer's event loop directly; it asks
    that loop to cancel its registered task with ``call_soon_threadsafe``.
    Synchronous handlers are intentionally absent because Python cannot safely
    interrupt a running thread — the worker's bounded hard-exit path handles
    those by letting the broker redeliver their unacknowledged messages.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tasks: dict[int, tuple[asyncio.AbstractEventLoop, asyncio.Task]] = {}

    def register_current(self) -> int:
        """Register the current asyncio task and return its removal token."""
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        if task is None:  # pragma: no cover - asyncio always supplies one here
            raise RuntimeError("No current asyncio task to register")
        token = id(task)
        with self._lock:
            self._tasks[token] = (loop, task)
        return token

    def unregister(self, token: int) -> None:
        with self._lock:
            self._tasks.pop(token, None)

    def cancel_all(self) -> int:
        """Request cancellation on every active job's owning event loop."""
        with self._lock:
            tasks = list(self._tasks.values())

        requested = 0
        for loop, task in tasks:
            if task.done():
                continue
            try:
                loop.call_soon_threadsafe(task.cancel)
                requested += 1
            except RuntimeError:
                # The consumer completed and closed its loop between the
                # snapshot and this call. Its registry ``finally`` will remove
                # the stale entry; there is nothing left to cancel.
                continue
        return requested
