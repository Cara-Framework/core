"""
Smart Job Dispatcher.

Automatically decides whether to run jobs synchronously or dispatch to queue
based on execution context.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cara.queues.contracts import Queueable


class SmartDispatcher:
    """
    Smart dispatcher that respects execution context.

    Automatically runs jobs synchronously when in sync context,
    otherwise dispatches to queue.

    This eliminates the need for if/else blocks throughout the codebase.
    """

    @staticmethod
    async def dispatch(job: "Queueable", routing_key: str = None) -> Any:
        """
        Smart dispatch: sync execution or queue based on context.

        Args:
            job: Job instance to dispatch
            routing_key: Optional routing key for queue

        Returns:
            Job result if sync, None if queued

        Example:
            >>> job = MyJob(param=value)
            >>> await SmartDispatcher.dispatch(job, "my.routing.key")
            # Runs sync if in ExecutionContext.sync(), queues otherwise
        """
        # Check execution context
        from cara.context import ExecutionContext

        is_sync = ExecutionContext.is_sync()

        if is_sync:
            # Run synchronously
            return await job.handle()
        else:
            # Dispatch to queue
            params = SmartDispatcher.get_dispatch_params(job)
            dispatch_call = job.__class__.dispatch(**params)
            if routing_key:
                dispatch_call.withRoutingKey(routing_key)
            return None

    @staticmethod
    def get_dispatch_params(job: "Queueable") -> dict:
        """
        Extract dispatch parameters from job instance.

        Handles Pydantic models and other complex objects by converting them
        to serializable dictionaries.

        Args:
            job: Job instance

        Returns:
            Dict of parameters for dispatch
        """
        # Get all init parameters from job
        # Exclude internal attributes, queue-specific fields, and runtime objects
        excluded_keys = {
            "queue",
            "attempts",
            "routing_key",
            "connection",
            "delay",
            "timeout",
            "tries",
            "backoff",
            "kwargs",
            # Runtime objects that should be reconstructed by the job
            "job_metadata",
            "job_context",
            "repository",
        }

        params = {}
        if hasattr(job, "__dict__"):
            for key, value in job.__dict__.items():
                if not key.startswith("_") and key not in excluded_keys:
                    params[key] = value

            # Special handling: if job has kwargs dict, merge it into params
            # This ensures all init parameters are passed correctly
            if "kwargs" in job.__dict__ and isinstance(job.__dict__["kwargs"], dict):
                for k, v in job.__dict__["kwargs"].items():
                    if k not in params and k not in excluded_keys:
                        params[k] = v

        return params
