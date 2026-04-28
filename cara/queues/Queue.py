"""
Queue Manager for the Cara framework.

This module provides the Queue class, which manages job queues and delegates queue operations to
registered driver instances.
"""

import asyncio
from typing import Any, Dict, Optional

from cara.exceptions import DriverNotRegisteredException
from cara.queues.contracts import Queue, ShouldQueue


class Queue:
    """
    Central queue manager.

    Holds registered drivers, delegates operations, and has dispatch().
    """

    def __init__(self, application, default_driver: str):
        self.application = application
        self._drivers: Dict[str, Queue] = {}
        self._default_driver: str = default_driver

    def add_driver(self, name: str, driver: Queue) -> None:
        self._drivers[name] = driver

    def driver(self, name: Optional[str] = None) -> Queue:
        chosen = name or self._default_driver
        inst = self._drivers.get(chosen)
        if not inst:
            raise DriverNotRegisteredException(
                f"Queue driver '{chosen}' is not registered."
            )
        return inst

    def push(
        self,
        *jobs: Any,
        driver_name: Optional[str] = None,
        **options: Any,
    ):
        """Push jobs to queue and return job ID(s) for tracking."""
        driver = self.driver(driver_name)
        return driver.push(*jobs, options=options)

    def consume(
        self,
        driver_name: Optional[str] = None,
        **options: Any,
    ) -> None:
        driver = self.driver(driver_name)
        driver.consume(options=options)

    def retry(
        self,
        driver_name: Optional[str] = None,
        **options: Any,
    ) -> None:
        driver = self.driver(driver_name)
        driver.retry(options=options)

    def chain(
        self,
        jobs: list,
        driver_name: Optional[str] = None,
        **options: Any,
    ) -> None:
        driver = self.driver(driver_name)
        driver.chain(jobs, options=options)

    def batch(
        self,
        *jobs: Any,
        driver_name: Optional[str] = None,
        **options: Any,
    ) -> None:
        driver = self.driver(driver_name)
        driver.batch(*jobs, options=options)

    def schedule(
        self,
        job: Any,
        when: Any,
        driver_name: Optional[str] = None,
        **options: Any,
    ) -> None:
        driver = self.driver(driver_name)
        driver.schedule(job, when, options=options)

    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a specific job by ID.

        Args:
            job_id: Job identifier to cancel

        Returns:
            bool: True if job was cancelled, False if not found
        """
        from cara.queues.JobStateManager import get_job_state_manager

        return get_job_state_manager().cancel_job(job_id)

    def cancel_jobs_by_context(
        self, context_filter: callable, reason: str = "Job superseded"
    ) -> int:
        """
        Cancel jobs based on context filter.

        Args:
            context_filter: Function that returns True for jobs to cancel
            reason: Reason for cancellation

        Returns:
            int: Number of jobs cancelled
        """
        from cara.queues.JobStateManager import get_job_state_manager

        return get_job_state_manager().cancel_jobs_by_context(context_filter, reason)

    def get_active_jobs(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all active jobs.

        Returns:
            dict: Active jobs with their states
        """
        from cara.queues.JobStateManager import get_job_state_manager

        return get_job_state_manager().get_active_jobs()

    def later(
        self,
        delay: Any,
        job: Any,
        driver_name: Optional[str] = None,
        **options: Any,
    ):
        """Dispatch a job with a delay — Laravel ``Queue::later()`` parity.

        Delegates to the active driver's ``later()`` method if available,
        otherwise falls back to ``schedule()``/``dispatchAfter()``.

        Args:
            delay: Delay in seconds (or a ``pendulum.Duration``).
            job: Queueable job instance.
            driver_name: Optional driver name override.
        """
        drv = self.driver(driver_name)
        if hasattr(drv, "later"):
            return drv.later(delay, job, options)
        # Fallback for drivers that only expose schedule()
        return self.schedule(job, delay, driver_name=driver_name, **options)

    def dispatch(
        self,
        job: Any,
        *args: Any,
        driver_name: Optional[str] = None,
        **kwargs: Any,
    ):
        """
        Laravel-style job dispatch - returns job ID for tracking.

        Dispatch a job:
         - If job is a class subclassing ShouldQueue, instantiate with args/kwargs and push to queue.
         - If job is an instance implementing ShouldQueue, push directly.
         - Otherwise, instantiate (with args/kwargs) or use instance and call handle() synchronously.
        Usage:
            job_id = Queue.dispatch(SomeJobClass, payload)
            job_id = Queue.dispatch(SomeJobClass, payload, driver_name="database")
        """
        app = self.application

        if isinstance(job, type) and issubclass(job, ShouldQueue):
            if hasattr(app, "make") and not args and not kwargs:
                instance = app.make(job)
            else:
                instance = job(*args, **kwargs)
            return self.push(instance, driver_name=driver_name)

        if not isinstance(job, type) and isinstance(job, ShouldQueue):
            return self.push(job, driver_name=driver_name)

        # Synchronous path: only for non-ShouldQueue jobs, where the
        # caller is explicitly running the handler inline. The previous
        # implementation also fell through to this path when
        # ``DriverNotRegisteredException`` bubbled up from ``push()`` —
        # which silently downgraded a queued ``ShouldQueue`` job into
        # an inline request-thread call, blowing up p99 latency and
        # bypassing every retry/idempotency guarantee the queue is
        # there to provide. We now let driver-configuration errors
        # surface to the caller; explicit sync execution is available
        # via ``dispatchNow`` / ``ExecutionContext.sync()``.
        if isinstance(job, type):
            if hasattr(app, "make") and not args and not kwargs:
                instance = app.make(job)
            else:
                instance = job(*args, **kwargs)
        else:
            instance = job

        if hasattr(instance, "handle") and callable(getattr(instance, "handle")):
            if hasattr(app, "call"):
                result = app.call(instance.handle)
            else:
                result = instance.handle()
            # Guard against accidentally dropping a coroutine from an
            # async handle() when no container is available.
            if asyncio.iscoroutine(result):
                result.close()
                raise TypeError(
                    f"Job {instance.__class__.__name__}.handle() is async "
                    f"but was dispatched in a sync context without an application container."
                )
            return result  # Return handle result for sync execution
        else:
            raise ValueError(f"Cannot dispatch job: {job!r} has no handle()")

    def dispatchAfter(
        self,
        job: Any,
        delay: Any,
        *args: Any,
        driver_name: Optional[str] = None,
        **kwargs: Any,
    ):
        """Laravel-style delayed job dispatch."""
        if isinstance(job, type) and issubclass(job, ShouldQueue):
            if hasattr(self.application, "make") and not args and not kwargs:
                instance = self.application.make(job)
            else:
                instance = job(*args, **kwargs)
            return self.schedule(instance, delay, driver_name=driver_name)
        elif not isinstance(job, type) and isinstance(job, ShouldQueue):
            return self.schedule(job, delay, driver_name=driver_name)
        else:
            raise ValueError(f"dispatchAfter requires a ShouldQueue job, got: {job!r}")

    def dispatchNow(
        self,
        job: Any,
        *args: Any,
        **kwargs: Any,
    ):
        """Laravel-style immediate job execution."""
        if isinstance(job, type):
            if hasattr(self.application, "make") and not args and not kwargs:
                instance = self.application.make(job)
            else:
                instance = job(*args, **kwargs)
        else:
            instance = job

        if hasattr(instance, "handle") and callable(getattr(instance, "handle")):
            if hasattr(self.application, "call"):
                return self.application.call(instance.handle)
            result = instance.handle()
            if asyncio.iscoroutine(result):
                result.close()
                raise TypeError(
                    f"Job {instance.__class__.__name__}.handle() is async "
                    f"but was dispatched via dispatchNow without an application container."
                )
            return result
        else:
            raise ValueError(f"Cannot execute job: {job!r} has no handle() method")

    # Python-style aliases for Laravel-named APIs above.
    def dispatch_after(
        self,
        job: Any,
        delay: Any,
        *args: Any,
        driver_name: Optional[str] = None,
        **kwargs: Any,
    ):
        """Python naming alias for :meth:`dispatchAfter`."""
        return self.dispatchAfter(
            job, delay, *args, driver_name=driver_name, **kwargs
        )

    def dispatch_now(self, job: Any, *args: Any, **kwargs: Any):
        """Python naming alias for :meth:`dispatchNow`."""
        return self.dispatchNow(job, *args, **kwargs)
