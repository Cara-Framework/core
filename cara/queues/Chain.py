"""Job chaining — run jobs sequentially with failure handling.

Jobs are dispatched and run in sequence. When job N finishes successfully,
job N+1 is immediately dispatched. If any job fails, remaining jobs are not
dispatched and the catch callback runs.

The runner persists its progress (``cara:chain:<id>``) so that when the
queue redelivers the runner message after a worker crash mid-chain, the
already-completed steps are skipped rather than re-run. Without this,
non-idempotent steps (e.g. scrape → validate → consolidate) could fire
their side-effects twice for the same input.

Usage:
    from cara.queues import Chain

    Chain([Job1(...), Job2(...), Job3(...)]).dispatch()

    Chain([...]) \\
        .catch(lambda exc, failed_job: log_failure(exc)) \\
        .dispatch()
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable

from cara.queues.contracts import Queueable, ShouldQueue


def _chain_progress_key(chain_id: str) -> str:
    return f"cara:chain:progress:{chain_id}"


class ChainRunnerJob(ShouldQueue, Queueable):
    """Internal job that executes a chain of jobs sequentially."""

    # How long a chain's progress marker survives in cache. 24h is long
    # enough to cover a worker outage + retry storm without keeping
    # stale chain state forever.
    CHAIN_PROGRESS_TTL = 24 * 60 * 60

    def __init__(
        self,
        jobs: list[Queueable],
        catch_callback: Callable[[Exception, Queueable], None] | None = None,
        chain_id: str | None = None,
    ):
        """Initialize chain runner.

        Args:
            jobs: List of job instances to run in sequence.
            catch_callback: Optional callback(exc, failed_job) on failure.
            chain_id: Stable identifier so a redelivered chain runner
                resumes from the last completed step instead of restarting.
        """
        super().__init__()
        self.queue = "default"
        self.jobs = jobs
        self.catch_callback = catch_callback
        self.chain_id = chain_id or uuid.uuid4().hex

    def _load_progress(self) -> int:
        """Return the index of the next step to run (0 on first delivery)."""
        try:
            from cara.facades import Cache

            value = Cache.get(_chain_progress_key(self.chain_id))
            if value is None:
                return 0
            return int(value)
        except Exception:
            return 0

    def _save_progress(self, next_index: int) -> None:
        try:
            from cara.facades import Cache

            Cache.put(
                _chain_progress_key(self.chain_id),
                next_index,
                self.CHAIN_PROGRESS_TTL,
            )
        except Exception:
            pass

    def _clear_progress(self) -> None:
        try:
            from cara.facades import Cache

            Cache.forget(_chain_progress_key(self.chain_id))
        except Exception:
            pass

    async def handle(self) -> None:
        """Execute all jobs in sequence, resuming from the last completed step."""
        start_at = self._load_progress()

        for index, job in enumerate(self.jobs):
            if index < start_at:
                continue
            try:
                app = getattr(job, "_app", None)
                if app is not None and hasattr(app, "call"):
                    result = app.call(job.handle)
                elif asyncio.iscoroutinefunction(job.handle):
                    result = await job.handle()
                else:
                    result = job.handle()

                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                if self.catch_callback:
                    self.catch_callback(e, job)
                raise

            # Persist progress so a redelivery of this runner skips the
            # just-completed step. The TTL caps stale state if the chain
            # is abandoned entirely.
            self._save_progress(index + 1)

        self._clear_progress()


class Chain:
    """Fluent API for chaining jobs."""

    def __init__(self, jobs: list[Queueable]):
        """Initialize chain with list of jobs.

        Args:
            jobs: List of job instances to chain together.
        """
        self.jobs = jobs
        self.catch_callback: Callable | None = None

    def catch(self, callback: Callable[[Exception, Queueable], None]) -> Chain:
        """Set failure callback.

        Args:
            callback: Callable(exc, failed_job) invoked if any job fails.

        Returns:
            Self for chaining.
        """
        self.catch_callback = callback
        return self

    def dispatch(self) -> None:
        """Dispatch the chain as a single ChainRunnerJob.

        The entire chain runs in sequence when the ChainRunnerJob is processed.
        """
        from cara.facades import Queue

        runner = ChainRunnerJob(self.jobs, self.catch_callback)
        Queue.push(runner)
