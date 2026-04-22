"""Job chaining — run jobs sequentially with failure handling.

Jobs are dispatched and run in sequence. When job N finishes successfully,
job N+1 is immediately dispatched. If any job fails, remaining jobs are not
dispatched and the catch callback runs.

Usage:
    from cara.queues import Chain

    Chain([Job1(...), Job2(...), Job3(...)]).dispatch()

    Chain([...]) \\
        .catch(lambda exc, failed_job: log_failure(exc)) \\
        .dispatch()
"""

import asyncio
from typing import Callable, List, Optional, Any

from cara.queues.contracts import ShouldQueue, Queueable


class ChainRunnerJob(ShouldQueue, Queueable):
    """Internal job that executes a chain of jobs sequentially."""

    def __init__(
        self,
        jobs: List[Queueable],
        catch_callback: Optional[Callable[[Exception, Queueable], None]] = None
    ):
        """Initialize chain runner.

        Args:
            jobs: List of job instances to run in sequence.
            catch_callback: Optional callback(exc, failed_job) on failure.
        """
        super().__init__()
        self.queue = "default"
        self.jobs = jobs
        self.catch_callback = catch_callback

    async def handle(self) -> None:
        """Execute all jobs in sequence."""
        for job in self.jobs:
            try:
                # Handle both sync and async job.handle()
                if asyncio.iscoroutinefunction(job.handle):
                    await job.handle()
                else:
                    result = job.handle()
                    if asyncio.iscoroutine(result):
                        await result
            except Exception as e:
                if self.catch_callback:
                    self.catch_callback(e, job)
                raise


class Chain:
    """Fluent API for chaining jobs."""

    def __init__(self, jobs: List[Queueable]):
        """Initialize chain with list of jobs.

        Args:
            jobs: List of job instances to chain together.
        """
        self.jobs = jobs
        self.catch_callback: Optional[Callable] = None

    def catch(self, callback: Callable[[Exception, Queueable], None]) -> "Chain":
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
