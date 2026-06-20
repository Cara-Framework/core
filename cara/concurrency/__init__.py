"""Concurrency primitives — deduplicated background tasks (more to come).

Generic, framework-level concurrency helpers. Apps pass their own
cache keys and coroutine factories; cara owns the dedup + cleanup
plumbing.
"""

from .BackgroundTasks import schedule_deduped_task

__all__ = ["schedule_deduped_task"]
