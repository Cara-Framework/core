"""
Async Queue Driver for the Cara framework.

Immediate asynchronous execution without queuing.
"""

import asyncio
import uuid
from typing import Any, Dict, List, Union

from cara.exceptions import QueueException
from cara.queues.contracts.Queue import Queue
from cara.queues.job_instantiation import instantiate_job
from cara.support.Console import HasColoredOutput


class AsyncDriver(HasColoredOutput, Queue):
    # Strong refs for fire-and-forget tasks. ``asyncio.create_task``
    # only weakly tracks tasks in the loop registry; without a strong
    # reference the GC can collect mid-flight tasks. Tasks remove
    # themselves from this set on completion.
    _pending_tasks: "set[asyncio.Task]" = set()

    @classmethod
    def _track(cls, task: asyncio.Task) -> None:
        cls._pending_tasks.add(task)
        task.add_done_callback(cls._pending_tasks.discard)

    """
    Async queue driver for immediate execution.

    Features:
    - Immediate job execution without queuing
    - Support for both sync and async methods
    - Job tracking with unique IDs
    - No persistence or retry support
    """

    driver_name = "async"

    def __init__(self, application, options: Dict[str, Any]):
        self.application = application
        self.options = options

    def push(self, *jobs: Any, options: Dict[str, Any]) -> Union[str, List[str]]:
        """Execute jobs immediately and return job ID(s) for tracking."""
        merged_opts = {**self.options, **options}
        job_ids = []

        for job in jobs:
            # Generate unique job ID for tracking
            job_id = str(uuid.uuid4())
            job_ids.append(job_id)

            # Execute job immediately
            self._execute_job(job, merged_opts, job_id)

        return job_ids[0] if len(job_ids) == 1 else job_ids

    def batch(self, *jobs: Any, options: Dict[str, Any]) -> None:
        """Batch execution: execute all jobs immediately."""
        self.push(*jobs, options=options)

    def chain(self, jobs: list, options: Dict[str, Any]) -> None:
        """Chain execution: execute jobs in sequence."""
        for job in jobs:
            self.push(job, options=options)

    def schedule(self, job: Any, when: Any, options: Dict[str, Any]) -> None:
        """Scheduling not supported; runs immediately."""
        self.push(job, options=options)

    def consume(self, options: Dict[str, Any]) -> None:
        """Consume not supported for async driver."""
        raise QueueException("AsyncDriver.consume() not supported.")

    def retry(self, options: Dict[str, Any]) -> None:
        """Retry not supported for async driver."""
        raise QueueException("AsyncDriver.retry() not supported.")

    def _execute_job(self, job: Any, options: Dict[str, Any], job_id: str):
        """Execute a single job immediately."""
        try:
            callback = options.get("callback", "handle")
            init_args = options.get("args", ())

            instance = instantiate_job(self.application, job, init_args)

            # Get callback method
            method_to_call = getattr(instance, callback, None)
            if not callable(method_to_call):
                raise AttributeError(f"Callback '{callback}' not found on {instance!r}")

            # Execute synchronously or asynchronously. Async-path tasks
            # are tracked in ``_pending_tasks`` so the GC can't drop
            # them between dispatch and completion — see class-level
            # docstring on ``_track``.
            if asyncio.iscoroutinefunction(method_to_call):
                if hasattr(self.application, "call"):
                    AsyncDriver._track(asyncio.create_task(
                        self.application.call(method_to_call, *init_args)
                    ))
                else:
                    AsyncDriver._track(asyncio.create_task(method_to_call(*init_args)))
            else:
                if hasattr(self.application, "call"):
                    self.application.call(method_to_call, *init_args)
                else:
                    method_to_call(*init_args)

            self.success(f"AsyncDriver: Job executed successfully (ID: {job_id})")

        except Exception as e:
            self.danger(f"AsyncDriver: Job failed (ID: {job_id}): {str(e)}")

            # Call failed method if exists
            if hasattr(instance, "failed"):
                try:
                    instance.failed({"job_id": job_id}, str(e))
                except Exception as inner:
                    self.danger(
                        f"AsyncDriver: Exception in failed() (ID: {job_id}): {inner}"
                    )

            raise
