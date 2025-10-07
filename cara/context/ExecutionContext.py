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
    Global execution context manager.

    Provides thread-safe access to execution mode flags without
    passing them through every function call.

    Example:
        >>> with ExecutionContext.sync():
        ...     # All jobs will run synchronously in this context
        ...     job.handle()
    """

    @staticmethod
    def is_sync() -> bool:
        """Check if currently in sync execution mode."""
        return _sync_mode.get()

    @staticmethod
    def is_debug() -> bool:
        """Check if currently in debug mode."""
        return _debug_mode.get()

    @staticmethod
    def sync(debug: bool = False):
        """
        Context manager for synchronous execution.

        Args:
            debug: Enable debug mode

        Example:
            >>> with ExecutionContext.sync(debug=True):
            ...     # Code runs in sync mode with debug enabled
            ...     process_data()
        """
        return _ExecutionContextManager(sync=True, debug=debug)

    @staticmethod
    def async_mode(debug: bool = False):
        """
        Context manager for asynchronous execution.

        Args:
            debug: Enable debug mode
        """
        return _ExecutionContextManager(sync=False, debug=debug)

    @staticmethod
    def set_sync(value: bool):
        """Set sync mode (use context manager instead when possible)."""
        _sync_mode.set(value)

    @staticmethod
    def set_debug(value: bool):
        """Set debug mode (use context manager instead when possible)."""
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
