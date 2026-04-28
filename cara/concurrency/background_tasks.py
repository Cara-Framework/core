"""Cache-deduplicated background-task spawner.

Used by request-path code that wants to kick off slow work
(AI generation, email composition, full-text re-indexing) without
blocking the response. The pattern: caller hits a cache miss,
returns a 202 stub, and calls :func:`schedule_deduped_task` to run
the real work in the background.

Why dedup: a single hot endpoint can fire 10 concurrent retries
before the first generation finishes. The Cache sentinel collapses
all of them onto one inflight call.

Generic — no domain knowledge. Apps pass any zero-arg coroutine
factory and supply a dedup key under their own naming convention
(e.g. ``"smart_completion:user:42:inflight"``).
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Set

from cara.facades import Cache, Log


# Strong references to in-flight tasks so the GC doesn't collect them
# before completion. Each task removes itself via a done-callback.
_background_tasks: Set[asyncio.Task] = set()

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
    matching task is already inflight (caller should just return its
    202 stub and let the existing task warm the cache).

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
          the cache still gets warmed.
    """
    try:
        if Cache.has(dedup_key):
            return False
    except Exception as e:
        Log.warning(f"[{label}] inflight check failed for {dedup_key}: {e}")
        # Treat as "not inflight" rather than refuse to dispatch —
        # better to risk a duplicate run than to never generate.

    try:
        Cache.put(dedup_key, "1", inflight_ttl)
    except Exception as e:
        Log.warning(f"[{label}] inflight write failed for {dedup_key}: {e}")
        # Continue anyway — losing the dedup is better than skipping work.

    async def _run() -> None:
        try:
            await coro_factory()
        except Exception as exc:
            Log.warning(
                f"[{label}] background task failed for {dedup_key}: "
                f"{exc.__class__.__name__}: {exc}"
            )
        finally:
            # Forget the inflight sentinel so a retry can re-trigger
            # immediately if the work itself failed silently. The
            # success path's own ``Cache.remember`` writes the real
            # cached value with its own TTL anyway.
            try:
                Cache.forget(dedup_key)
            except Exception as e:
                Log.debug(f"[{label}] inflight cleanup failed for {dedup_key}: {e}")

    try:
        task = asyncio.create_task(_run())
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)
        return True
    except RuntimeError as e:
        # No running loop — possible during sync test contexts. Run
        # synchronously as a fallback so the cache still gets warmed.
        Log.debug(f"[{label}] no event loop, running sync: {e}")
        try:
            asyncio.run(_run())
        except Exception as inner:
            Log.warning(f"[{label}] sync fallback failed for {dedup_key}: {inner}")
        return True


__all__ = ["schedule_deduped_task"]
