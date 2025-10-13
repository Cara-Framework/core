"""
Execution Context Manager.

Manages global execution context for the application, including sync/async mode.
"""

from contextvars import ContextVar
from typing import Optional

# Context variable for sync mode (thread-safe)
_sync_mode: ContextVar[bool] = ContextVar("sync_mode", default=False)
_debug_mode: ContextVar[bool] = ContextVar("debug_mode", default=False)


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
    def sync(debug: bool = False):
        """
        Context manager for synchronous execution.

        Jobs will run immediately instead of being queued.
        Useful for:
        - CLI commands with --sync flag
        - Unit tests
        - Debugging

        Args:
            debug: Enable debug logging

        Example:
            >>> with ExecutionContext.sync(debug=True):
            ...     await Bus.dispatch(CollectProductJob(asin="B089DR29T6"))
            ...     # Job runs immediately with debug logs
        """
        return _ExecutionContextManager(sync=True, debug=debug)

    @staticmethod
    def queue(debug: bool = False):
        """
        Context manager for queue execution (explicit).

        Jobs will be dispatched to queue (RabbitMQ/Redis/Database).
        This is the default behavior, use this for clarity.

        Args:
            debug: Enable debug logging

        Example:
            >>> with ExecutionContext.queue():
            ...     await Bus.dispatch(job)  # Explicitly queue
        """
        return _ExecutionContextManager(sync=False, debug=debug)

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

    def __init__(self, sync: bool, debug: bool):
        self.sync = sync
        self.debug = debug
        self.sync_token: Optional[object] = None
        self.debug_token: Optional[object] = None

    def __enter__(self):
        """Enter context."""
        self.sync_token = _sync_mode.set(self.sync)
        self.debug_token = _debug_mode.set(self.debug)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and restore previous values."""
        if self.sync_token is not None:
            _sync_mode.reset(self.sync_token)
        if self.debug_token is not None:
            _debug_mode.reset(self.debug_token)
        return False
