"""A small, dedicated thread pool that refuses work instead of queueing it."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from cara.context.ExecutionContext import ExecutionContext
from cara.context.ThreadLaneBusy import ThreadLaneBusy

_T = TypeVar("_T")


class ThreadLane:
    """``size`` threads for blocking calls whose duration someone else decides.

    :meth:`ExecutionContext.run_in_thread` uses the event loop's default
    executor, which also runs every request's database work. A call an
    outside party can hold open — fetching a URL a user typed — must not
    be able to occupy those threads: a few dozen slow peers and every
    request in the process queues behind them. A lane gives such calls
    their own threads, and when all of them are busy :meth:`run` raises
    :class:`ThreadLaneBusy` at once rather than queueing, so a flood is
    refused at the door instead of building a backlog.

    A slot is held until the call itself finishes, not until the awaiting
    task does: a cancelled await (the client hung up) cannot free a slot
    whose thread is still blocked.
    """

    def __init__(self, name: str, *, size: int) -> None:
        if int(size) <= 0:
            raise ValueError("thread lane size must be positive")
        self.name = str(name)
        self.size = int(size)
        self._slots = threading.BoundedSemaphore(self.size)
        self._executor = ThreadPoolExecutor(
            max_workers=self.size, thread_name_prefix=f"lane-{self.name}"
        )

    async def run(self, func: Callable[..., _T], /, *args: Any, **kwargs: Any) -> _T:
        """Run ``func`` on a lane thread, or raise :class:`ThreadLaneBusy`.

        Context vars propagate exactly as with
        :meth:`ExecutionContext.run_in_thread`.
        """
        if not self._slots.acquire(blocking=False):
            raise ThreadLaneBusy(self.name, self.size)
        try:
            future = self._executor.submit(
                ExecutionContext.bind_to_thread(func, *args, **kwargs)
            )
        except BaseException:
            self._slots.release()
            raise
        # Released by the future itself — on completion, or on a cancel
        # that landed before a thread picked the call up.
        future.add_done_callback(lambda _done: self._slots.release())
        return await asyncio.wrap_future(future)


__all__ = ["ThreadLane"]
