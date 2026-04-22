"""Job batching — dispatch multiple jobs with completion tracking.

Jobs are dispatched in parallel (fire-and-forget). In the minimal implementation,
the .then() callback runs client-side after all jobs have been pushed to the queue.

Usage:
    from cara.queues import Batch

    Batch([Job1(), Job2(), ...]) \\
        .then(lambda batch_result: print("All done")) \\
        .dispatch()

TODO: Full completion tracking — add BatchAware mixin so jobs decrement a
      Cache-backed pending counter, and when counter hits 0, dispatch a
      BatchFinalizerJob to run .then(). Currently this is a fire-and-forget
      with eager client-side callback.
"""

import uuid
from typing import Callable, List, Optional, Any

from cara.queues.contracts import Queueable


class Batch:
    """Fluent API for batching jobs."""

    def __init__(self, jobs: List[Queueable]):
        """Initialize batch with list of jobs.

        Args:
            jobs: List of job instances to dispatch in parallel.
        """
        self.jobs = jobs
        self.batch_id = str(uuid.uuid4())
        self.then_callback: Optional[Callable[[Any], None]] = None
        self.catch_callback: Optional[Callable[[Exception, Queueable], None]] = None

    def then(self, callback: Callable[[Any], None]) -> "Batch":
        """Set completion callback (runs client-side, eager).

        Args:
            callback: Callable(batch_result) invoked after all jobs dispatched.

        Returns:
            Self for chaining.
        """
        self.then_callback = callback
        return self

    def catch(self, callback: Callable[[Exception, Queueable], None]) -> "Batch":
        """Set failure callback (not used in minimal implementation).

        Args:
            callback: Callable(exc, failed_job) for failure handling.

        Returns:
            Self for chaining.

        Note:
            In minimal implementation, catch is recorded but not invoked.
            Full completion tracking (TODO) will use this callback.
        """
        self.catch_callback = callback
        return self

    def dispatch(self) -> None:
        """Dispatch all jobs in parallel (fire-and-forget).

        In the minimal implementation, jobs are pushed to queue immediately
        and then_callback is invoked client-side. There is no tracking of
        job completion.

        TODO: Implement full completion tracking via Cache-backed pending
              counter and BatchFinalizerJob.
        """
        from cara.facades import Queue

        # Push all jobs to queue in parallel
        for job in self.jobs:
            try:
                Queue.push(job)
            except Exception as e:
                if self.catch_callback:
                    self.catch_callback(e, job)
                else:
                    raise

        # Client-side eager callback (not waiting for actual completion)
        if self.then_callback:
            self.then_callback({"batch_id": self.batch_id, "job_count": len(self.jobs)})
