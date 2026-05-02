"""
Execution Context Manager.

Manages global execution context for the application, including sync/async mode.
"""

from contextvars import ContextVar
from typing import Optional

# Context variable for sync mode (thread-safe)
_sync_mode: ContextVar[bool] = ContextVar("sync_mode", default=False)
_debug_mode: ContextVar[bool] = ContextVar("debug_mode", default=False)
_job_id: ContextVar[Optional[str]] = ContextVar("job_id", default=None)
_batch_id: ContextVar[Optional[str]] = ContextVar("batch_id", default=None)
_correlation_id: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


class ExecutionContext:
    """
    Thread-safe execution context manager.

    Manages global execution flags using context variables.
    Used by Bus to determine sync vs async job dispatch.

    Example:
        >>> # Sync mode (CLI with --sync, testing)
        >>> with ExecutionContext.sync():
        ...     await Bus.dispatch(job)  # Runs immediately

        >>> # Queue mode (default, workers)
        >>> with ExecutionContext.queue():
        ...     await Bus.dispatch(job)  # Dispatches to RabbitMQ
    """

    @staticmethod
    def is_sync() -> bool:
        """
        Check if currently in sync execution mode.

        Returns:
            True if jobs should run immediately, False if they should queue
        """
        return _sync_mode.get()

    @staticmethod
    def is_debug() -> bool:
        """
        Check if currently in debug mode.

        Returns:
            True if debug logging is enabled
        """
        return _debug_mode.get()

    @staticmethod
    def get_job_id() -> Optional[str]:
        """
        Get current job ID from context.

        Returns:
            Current job ID or None
        """
        return _job_id.get()

    @staticmethod
    def set_job_id(job_id: str):
        """Set job ID in context."""
        _job_id.set(job_id)

    @staticmethod
    def get_batch_id() -> Optional[str]:
        """Get current batch ID — groups related jobs dispatched together."""
        return _batch_id.get()

    @staticmethod
    def set_batch_id(batch_id: str):
        """Set batch ID in context."""
        _batch_id.set(batch_id)

    @staticmethod
    def get_correlation_id() -> Optional[str]:
        """Get correlation ID — traces a causal chain across job generations."""
        return _correlation_id.get()

    @staticmethod
    def set_correlation_id(correlation_id: str):
        """Set correlation ID in context."""
        _correlation_id.set(correlation_id)

    @staticmethod
    def sync(debug: bool = False, job_id: Optional[str] = None):
        """
        Context manager for synchronous execution.

        Jobs will run immediately instead of being queued.
        Useful for:
        - CLI commands with --sync flag
        - Unit tests
        - Debugging

        Args:
            debug: Enable debug logging
            job_id: Optional job ID to track across pipeline

        Example:
            >>> with ExecutionContext.sync(debug=True, job_id="collect_123"):
            ...     await Bus.dispatch(CollectProductJob(asin="B089DR29T6"))
            ...     # Job runs immediately with debug logs
        """
        return _ExecutionContextManager(sync=True, debug=debug, job_id=job_id)

    @staticmethod
    def queue(debug: bool = False, job_id: Optional[str] = None):
        """
        Context manager for queue execution (explicit).

        Jobs will be dispatched to queue (RabbitMQ/Redis/Database).
        This is the default behavior, use this for clarity.

        Args:
            debug: Enable debug logging
            job_id: Optional job ID to track across pipeline

        Example:
            >>> with ExecutionContext.queue():
            ...     await Bus.dispatch(job)  # Explicitly queue
        """
        return _ExecutionContextManager(sync=False, debug=debug, job_id=job_id)

    @staticmethod
    def set_sync(value: bool):
        """
        Set sync mode directly (not recommended).

        Prefer using context managers (sync() or queue()) instead.

        Args:
            value: True for sync mode, False for queue mode
        """
        _sync_mode.set(value)

    @staticmethod
    def set_debug(value: bool):
        """
        Set debug mode directly (not recommended).

        Prefer using context managers with debug parameter.

        Args:
            value: True to enable debug logging
        """
        _debug_mode.set(value)


class _ExecutionContextManager:
    """Internal context manager implementation."""

    def __init__(
        self,
        sync: bool,
        debug: bool,
        job_id: Optional[str] = None,
        batch_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ):
        self.sync = sync
        self.debug = debug
        self.job_id = job_id
        self.batch_id = batch_id
        self.correlation_id = correlation_id
        self._tokens: list = []

    def __enter__(self):
        self._tokens.append(("sync", _sync_mode.set(self.sync)))
        self._tokens.append(("debug", _debug_mode.set(self.debug)))
        if self.job_id is not None:
            self._tokens.append(("job_id", _job_id.set(self.job_id)))
        if self.batch_id is not None:
            self._tokens.append(("batch_id", _batch_id.set(self.batch_id)))
        if self.correlation_id is not None:
            self._tokens.append(("corr", _correlation_id.set(self.correlation_id)))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _var_map = {
            "sync": _sync_mode,
            "debug": _debug_mode,
            "job_id": _job_id,
            "batch_id": _batch_id,
            "corr": _correlation_id,
        }
        for name, token in reversed(self._tokens):
            _var_map[name].reset(token)
        self._tokens.clear()
        return False
