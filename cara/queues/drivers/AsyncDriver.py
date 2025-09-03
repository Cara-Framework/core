"""
Async Queue Driver for the Cara framework.

This module implements a queue driver for immediate asynchronous execution of jobs.
"""

import asyncio
import inspect
import os
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Dict, List, Union

from cara.exceptions import QueueException
from cara.queues.contracts.Queue import Queue
from cara.support.Console import HasColoredOutput


class AsyncDriver(HasColoredOutput, Queue):
    """
    Async queue driver.

    Immediately executes jobs asynchronously without queuing.
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

        # Return single job ID if only one job, otherwise return list
        return job_ids[0] if len(job_ids) == 1 else job_ids

    def _execute_job(self, job: Any, options: Dict[str, Any], job_id: str):
        """Execute a single job immediately."""
        try:
            callback = options.get("callback", "handle")
            init_args = options.get("args", ())

            # Determine instance
            if inspect.isclass(job):
                if hasattr(self.application, "make") and not init_args:
                    try:
                        instance = self.application.make(job)
                    except Exception:
                        instance = job(*init_args)
                else:
                    instance = job(*init_args)
            else:
                instance = job

            # Call the callback method
            method_to_call = getattr(instance, callback, None)
            if not callable(method_to_call):
                raise AttributeError(f"Callback '{callback}' not found on {instance!r}")

            # Execute synchronously or asynchronously
            if asyncio.iscoroutinefunction(method_to_call):
                # Run async method
                asyncio.create_task(method_to_call(*init_args))
            else:
                # Run sync method
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
            raise e

    def consume(self, options: Dict[str, Any]) -> None:
        raise QueueException("AsyncDriver.consume() not supported.")

    def retry(self, options: Dict[str, Any]) -> None:
        raise QueueException("AsyncDriver.retry() not supported.")

    def chain(self, jobs: list, options: Dict[str, Any]) -> None:
        for job in jobs:
            self.push(job, options=options)

    def batch(self, *jobs: Any, options: Dict[str, Any]) -> None:
        self.push(*jobs, options=options)

    def schedule(self, job: Any, when: Any, options: Dict[str, Any]) -> None:
        """Scheduling not supported; runs immediately."""
        self.push(job, options=options)

    def _get_executor(self, mode: str, max_workers: int):
        if max_workers is None:
            max_workers = (os.cpu_count() or 1) * 5
        if mode == "threading":
            return ThreadPoolExecutor(max_workers=max_workers)
        elif mode == "multiprocess":
            return ProcessPoolExecutor(max_workers=max_workers)
        else:
            raise QueueException(f"Queue mode '{mode}' not recognized.")
