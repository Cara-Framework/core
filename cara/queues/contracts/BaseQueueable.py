"""
Base class for all queueable operations in Cara Framework.

This class eliminates code repetition across Mail, Notification, Job
and other queueable classes by providing common functionality.
"""

from __future__ import annotations

from typing import Any

from .Queueable import Queueable
from .ShouldQueue import ShouldQueue


class BaseQueueable(Queueable, ShouldQueue):
    """
    DRY base class for all queueable operations.

    Features:
    - Automatic initialization handling
    - Common display_name logic
    - Centralized queue configuration
    - Automatic serialization handling
    """

    # Every executable job must opt into one canonical queue explicitly.
    default_queue: str | None = None
    default_delay: int | None = None
    default_retry_attempts: int = 3

    def __init__(self, *args, **kwargs):
        """Automatic initialization for all queueable classes."""
        super().__init__()  # Initialize SerializesModels through Queueable
        self._setup_queueable_properties()
        self._initialize_data(*args, **kwargs)
        self._chained = False  # Track if this is a chained job

    def _setup_queueable_properties(self):
        """Initialize queueable-specific properties."""
        self._queue_name = self.default_queue
        self._delay = self.default_delay
        self._retry_attempts = self.default_retry_attempts

    def _initialize_data(self, *args, **kwargs):
        """Override this in subclasses for custom initialization."""
        pass

    @property
    def queue_name(self) -> str | None:
        """Get the queue name for this job."""
        return self._queue_name

    @queue_name.setter
    def queue_name(self, name: str):
        """Set the queue name for this job."""
        self._queue_name = name

    def delay(self, seconds: int) -> BaseQueueable:
        """Delay the job execution by specified seconds."""
        self._delay = seconds
        self._chained = True
        return self

    def attempts(self, count: int) -> BaseQueueable:
        """Set the number of retry attempts."""
        self._retry_attempts = count
        self._chained = True
        return self

    def on_queue(self, queue: str) -> BaseQueueable:
        """Set the canonical queue without dispatching."""
        self.queue_name = queue
        self._chained = True
        return self

    def display_name(self) -> str:
        """
        Generate display name for queue monitoring.
        Uses intelligent detection based on available properties.
        """
        class_name = self.__class__.__name__

        # For job classes with payload
        if hasattr(self, "payload") and isinstance(self.payload, dict):
            action = self.payload.get("action", "job")
            return f"{class_name}: {action}"

        # For mail classes with user
        if hasattr(self, "user") and self.user:
            user_id = getattr(self.user, "email", getattr(self.user, "name", "unknown"))
            return f"{class_name}: {user_id}"

        # For notification classes with notification_type
        if hasattr(self, "notification_type"):
            return f"{class_name}: {self.notification_type}"

        # For classes with message attribute
        if hasattr(self, "message"):
            message_preview = str(self.message)[:30]
            return f"{class_name}: {message_preview}"

        # Default fallback
        return class_name

    def get_queue_options(self) -> dict[str, Any]:
        """Get queue configuration options."""
        options = {
            "queue": self.queue_name,
            "attempts": self._retry_attempts,
        }

        if self._delay is not None:
            options["delay"] = self._delay

        return options

    async def failed(
        self,
        job_data: Any,
        error: Exception,
        *,
        idempotency_key: str,
    ) -> None:
        """
        Handle job failure.

        Overrides must deduplicate external effects with ``idempotency_key``.
        The terminal-hook outbox retries after crashes with the same key.
        """
        if not isinstance(idempotency_key, str) or not idempotency_key:
            raise ValueError("failed() requires a non-empty idempotency_key.")
        # Import here to avoid circular imports
        try:
            from cara.facades import Log

            Log.error("%s failed: %s", self.__class__.__name__, str(error))
        except ImportError:
            # Silently fail if Log facade not available - this is a framework component
            pass
