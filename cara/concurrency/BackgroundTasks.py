"""Cache-deduplicated background-task spawner.

An in-process, LOSS-TOLERANT primitive: the work is an ``asyncio.Task``
in one API process, with no durable record anywhere. A pod restart, a
deploy or a crash ends it and nothing notices.

**Legal use — cache warming only.** Schedule work here only when ALL of
these hold:

* the work is idempotent and fully recomputable from persisted state,
* losing it costs nothing but a recomputation,
* the trigger RE-ARMS by itself — the caller re-probes the cache on the
  next request and re-schedules on a miss, so a dropped task self-heals,
* the caller returns real renderable content (or dispatches a durable
  job), NEVER a bare "queued"/202 acknowledgement that a client must
  poll for.

**Illegal use — anything the user is waiting on.** A user-triggered
server operation must expose durable operation state (``{queued,
coalesced, op}``); fire-and-forget is forbidden and a bare "queued" toast
is a bug (doctrine §8). That work goes to ``Bus.dispatch`` with a
durable operation record — ``commons/shared/runtime/ChannelOps.py`` is
the exemplar. Mutations never belong here at all: a lost mutation is
lost silently, and the caller cannot reload its state after a refresh.

Why dedup: a single hot endpoint can fire 10 concurrent retries
before the first generation finishes. The Cache sentinel collapses
all of them onto one inflight call. It is a best-effort collapse, not a
guarantee — on cache failure this module deliberately prefers running
over skipping — which is only safe because the work is idempotent.

Generic — no domain knowledge. Apps pass any zero-arg coroutine
factory and supply a dedup key under their own naming convention
(e.g. ``"smart_completion:user:42:inflight"``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from cara.facades import Cache, Log

# Strong references to in-flight tasks so the GC doesn't collect them
# before completion. Each task removes itself via a done-callback.
_background_tasks: set[asyncio.Task] = set()

# Default TTL for the "generation in flight" sentinel. Chosen to
# comfortably exceed most slow background calls (~5s observed for AI
# roundtrips) but short enough that a stuck dispatcher doesn't block
# retries forever. Caller can override per-call.
_DEFAULT_INFLIGHT_TTL_SECONDS = 30


def schedule_deduped_task(
    *,
    dedup_key: str,
    coro_factory: Callable[[], Awaitable[Any]],
    inflight_ttl: int = _DEFAULT_INFLIGHT_TTL_SECONDS,
    label: str = "background",
) -> bool:
    """Spawn a background task, deduplicated against a Cache sentinel.

    Returns ``True`` if a new task was scheduled, ``False`` if a
    matching task is already inflight. Either way the caller answers
    from what it can compute NOW and re-probes on the next request —
    see the module docstring for the contract this primitive is legal
    under.

    Args:
        dedup_key: Cache key that gates the inflight check. Convention
            is ``"<service>:<entity_kind>:<entity_id>:inflight"``.
        coro_factory: Zero-arg callable that returns an awaitable
            doing the actual work. Wrapped here in a try/except so an
            unhandled exception in the background task can't crash
            the worker.
        inflight_ttl: How long the sentinel persists, in seconds.
            Pick longer than the slowest expected work; on dispatch
            failure the key is dropped so retries can proceed
            immediately.
        label: Log label so operators can grep failures by service.

    Behaviour notes:
        * Sentinel cleanup runs in ``finally`` after the task body —
          on either success or failure — so a crashed task doesn't
          poison the dedup key for ``inflight_ttl`` seconds.
        * If no event loop is running (e.g. sync test context), the
          coroutine is executed synchronously via ``asyncio.run`` so
          the cache still gets warmed. That path BLOCKS the caller for
          the full duration of the work and must never be reached from
          a request path — a request path always has a running loop, so
          it is only reachable from loop-less/sync contexts.
    """
    # Atomic SET-NX with TTL: ``Cache.add`` returns True only when the
    # key did not previously exist. The earlier ``has`` + ``put``
    # pattern raced — two concurrent dispatches could both see "not
    # inflight" and both run the task. With ``add`` the loser sees
    # False and bails. ``add`` is mandated by ``cara.cache.contracts``,
    # so there is nothing to fall back TO; the retained two-step branch
    # was a second, racier implementation of the same behaviour that a
    # driver could silently select just by shadowing the attribute.
    try:
        acquired = bool(Cache.add(dedup_key, "1", inflight_ttl))
    except Exception as e:
        Log.warning("[%s] inflight add failed for %s: %s", label, dedup_key, e)
        # On cache failure prefer running over skipping.
        acquired = True
    if not acquired:
        return False

    async def _run() -> None:
        try:
            await coro_factory()
        except Exception as exc:
            Log.warning(
                "[%s] background task failed for %s: %s: %s",
                label,
                dedup_key,
                exc.__class__.__name__,
                exc,
            )
        finally:
            # Forget the inflight sentinel so a retry can re-trigger
            # immediately if the work itself failed silently. The
            # success path's own ``Cache.remember`` writes the real
            # cached value with its own TTL anyway.
            try:
                Cache.forget(dedup_key)
            except Exception as e:
                Log.debug("[%s] inflight cleanup failed for %s: %s", label, dedup_key, e)

    # Build the coroutine ONCE. ``create_task`` fails on ``get_running_loop``
    # before it ever touches the coroutine, so the same object is safe to hand
    # to ``asyncio.run`` on the fallback path — calling ``_run()`` a second
    # time left the first coroutine unstarted and unreferenced, which Python
    # reports as "coroutine was never awaited" noise on every sync-context use.
    pending = _run()

    try:
        task = asyncio.create_task(pending)
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)
        return True
    except RuntimeError as e:
        # No running loop — possible during sync test contexts. Run
        # synchronously as a fallback so the cache still gets warmed.
        Log.debug("[%s] no event loop, running sync: %s", label, e)
        try:
            asyncio.run(pending)
        except Exception as inner:
            Log.warning("[%s] sync fallback failed for %s: %s", label, dedup_key, inner)
        return True


__all__ = ["schedule_deduped_task"]
