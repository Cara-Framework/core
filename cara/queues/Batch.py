"""Job batching — dispatch multiple jobs with completion tracking.

Jobs are dispatched in parallel. A Cache-backed pending counter tracks
how many jobs remain. Each job decrements the counter when it finishes
(via the ``BatchAware`` mixin). When the counter reaches 0 the
``.then()`` callback is invoked by the last finishing job. If any job
fails, the ``.catch()`` callback fires once per failure.

Usage::

    from cara.queues import Batch

    Batch([Job1(), Job2(), ...]) \\
        .then(lambda result: print("All done:", result)) \\
        .catch(lambda exc, job: print("Failed:", job)) \\
        .dispatch()
"""

import uuid
from typing import Any, Callable, Dict, List, Optional

from cara.queues.contracts import Queueable

# Re-export the helper at module level.
__all__ = ["Batch", "BatchAware", "auto_dispatch_batch_completion"]


# ── Cache key helpers ─────────────────────────────────────────────────
def _batch_key(batch_id: str) -> str:
    return f"cara:batch:{batch_id}"


def _batch_pending_key(batch_id: str) -> str:
    return f"cara:batch:{batch_id}:pending"


def _batch_failed_key(batch_id: str) -> str:
    return f"cara:batch:{batch_id}:failed"


# ── Batch ─────────────────────────────────────────────────────────────

class Batch:
    """Fluent API for batching jobs with completion tracking."""

    # How long batch metadata stays in cache (seconds). After this TTL
    # any orphaned counter is auto-cleaned by the cache driver.
    BATCH_TTL = 86400  # 24 h

    def __init__(self, jobs: List[Queueable]):
        self.jobs = jobs
        self.batch_id = str(uuid.uuid4())
        self.then_callback: Optional[Callable[[Dict[str, Any]], None]] = None
        self.catch_callback: Optional[Callable[[Exception, Queueable], None]] = None

    def then(self, callback: Callable[[Dict[str, Any]], None]) -> "Batch":
        """Set completion callback — fires when ALL jobs finish successfully."""
        self.then_callback = callback
        return self

    def catch(self, callback: Callable[[Exception, Queueable], None]) -> "Batch":
        """Set failure callback — fires once per failed job."""
        self.catch_callback = callback
        return self

    def dispatch(self) -> str:
        """Dispatch all jobs with batch tracking.

        Returns:
            The ``batch_id`` so callers can poll status if desired.
        """
        from cara.facades import Cache, Queue, Log

        total = len(self.jobs)
        if total == 0:
            # Empty batch — fire then() immediately.
            if self.then_callback:
                self.then_callback({"batch_id": self.batch_id, "total": 0, "failed": 0})
            return self.batch_id

        # Store batch metadata in cache.
        Cache.put(_batch_key(self.batch_id), {
            "total": total,
            "then": self.then_callback is not None,
            "catch": self.catch_callback is not None,
        }, self.BATCH_TTL)
        Cache.put(_batch_pending_key(self.batch_id), total, self.BATCH_TTL)
        Cache.put(_batch_failed_key(self.batch_id), 0, self.BATCH_TTL)

        # Stamp each job with batch info so BatchAware can decrement.
        dispatched = 0
        for job in self.jobs:
            job._batch_id = self.batch_id
            job._batch_then_callback = self.then_callback
            job._batch_catch_callback = self.catch_callback
            try:
                Queue.push(job)
                dispatched += 1
            except Exception as e:
                Log.error(f"Batch {self.batch_id}: failed to dispatch {type(job).__name__}: {e}")
                if self.catch_callback:
                    self.catch_callback(e, job)
                # Decrement pending since this job will never run.
                _decrement_pending(self.batch_id, self.then_callback)

        Log.debug(
            f"Batch {self.batch_id}: dispatched {dispatched}/{total} jobs",
            category="cara.queue.batch",
        )
        return self.batch_id


# ── BatchAware mixin ──────────────────────────────────────────────────

class BatchAware:
    """Mixin for jobs that participate in batch tracking.

    Add this to a job class alongside ``Queueable``::

        class MyJob(Queueable, BatchAware):
            def handle(self):
                ...  # your logic
                self.batch_completed()  # at the end

    Or rely on the automatic hook — if the queue worker calls
    ``job.handle()`` and the job has ``_batch_id``, the counter is
    decremented automatically when ``handle()`` returns without error.
    """

    def batch_completed(self) -> None:
        """Signal that this job's batch work is done."""
        batch_id = getattr(self, "_batch_id", None)
        if not batch_id:
            return
        then_cb = getattr(self, "_batch_then_callback", None)
        _decrement_pending(batch_id, then_cb)

    def batch_failed(self, exc: Exception) -> None:
        """Signal that this job failed within a batch."""
        batch_id = getattr(self, "_batch_id", None)
        if not batch_id:
            return

        from cara.facades import Cache
        try:
            # Pass the batch TTL explicitly. If the initial ``put()``
            # missed (cache eviction, partial init, cold-start before
            # the batch row landed), bare ``increment`` would create a
            # key with no expiry and the failed-counter slowly fills
            # Redis. Explicit TTL guarantees the key dies with the batch.
            Cache.increment(_batch_failed_key(batch_id), 1, Batch.BATCH_TTL)
        except Exception:
            pass

        catch_cb = getattr(self, "_batch_catch_callback", None)
        if catch_cb:
            try:
                catch_cb(exc, self)
            except Exception:
                pass

        then_cb = getattr(self, "_batch_then_callback", None)
        _decrement_pending(batch_id, then_cb)


def auto_dispatch_batch_completion(instance: Any, exception: Optional[Exception] = None) -> None:
    """Worker-side hook — called by every queue driver after a job
    runs (success or failure).

    Looks for a ``_batch_id`` stamped on the instance by ``Batch.dispatch``;
    if present, calls ``batch_completed`` (success) or ``batch_failed``
    (failure). Without this hook, the batch counter never decremented
    unless the job manually called ``self.batch_completed()`` in its
    handle body — which most jobs don't, so ``then()`` callbacks
    never fired and orphan batch keys lived for the full TTL (24h).

    Defensive: a job without ``_batch_id`` is a no-op, anything else
    that raises is swallowed so a buggy ``batch_failed`` override can't
    crash the worker.
    """
    if instance is None:
        return
    if not getattr(instance, "_batch_id", None):
        return
    try:
        if exception is None:
            completed = getattr(instance, "batch_completed", None)
            if callable(completed):
                completed()
        else:
            failed = getattr(instance, "batch_failed", None)
            if callable(failed):
                failed(exception)
    except Exception:
        # Never let batch bookkeeping break the worker loop.
        pass


def _decrement_pending(batch_id: str, then_callback=None) -> None:
    """Atomically decrement the pending counter; fire then() when it hits 0."""
    from cara.facades import Cache, Log

    try:
        remaining = Cache.decrement(_batch_pending_key(batch_id))
    except Exception as e:
        Log.error(f"Batch {batch_id}: failed to decrement pending counter: {e}")
        return

    if remaining is not None and int(remaining) <= 0:
        # Last job — fire the then() callback.
        Log.info(f"Batch {batch_id}: all jobs completed", category="cara.queue.batch")

        meta = Cache.get(_batch_key(batch_id)) or {}
        failed_count = int(Cache.get(_batch_failed_key(batch_id)) or 0)

        if then_callback:
            try:
                then_callback({
                    "batch_id": batch_id,
                    "total": meta.get("total", 0),
                    "failed": failed_count,
                })
            except Exception as e:
                Log.error(f"Batch {batch_id}: then() callback raised: {e}")

        # Cleanup cache keys.
        for key in (_batch_key(batch_id), _batch_pending_key(batch_id), _batch_failed_key(batch_id)):
            try:
                Cache.forget(key)
            except Exception:
                pass
