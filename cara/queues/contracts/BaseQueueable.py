"""
Base class for all queueable operations in Cara Framework.

This class eliminates code repetition across Mail, Notification, Job
and other queueable classes by providing common functionality.
"""

from typing import Any, Dict, Optional

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

    # Default queue settings
    default_queue: str = "default"
    default_delay: Optional[int] = None
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
    def queue_name(self) -> str:
        """Get the queue name for this job."""
        return self._queue_name

    @queue_name.setter
    def queue_name(self, name: str):
        """Set the queue name for this job."""
        self._queue_name = name

    def delay(self, seconds: int) -> "BaseQueueable":
        """Delay the job execution by specified seconds."""
        self._delay = seconds
        self._chained = True
        return self

    def attempts(self, count: int) -> "BaseQueueable":
        """Set the number of retry attempts."""
        self._retry_attempts = count
        self._chained = True
        return self

    def on_queue(self, queue: str) -> "BaseQueueable":
        """
        Set the queue for this job and auto-dispatch (Laravel pattern).
        
        In Laravel, method chaining automatically queues the job.
        This maintains the same behavior.
        """
        self.queue_name = queue
        self._chained = True
        
        # Auto-dispatch when chaining is used (Laravel pattern)
        self._auto_dispatch()
        return self
        
    def _auto_dispatch(self):
        """Auto-dispatch job when method chaining is used (Laravel pattern)."""
        if not self._chained:
            return
            
        try:
            from cara.facades import Queue
            Queue.push(self)
        except Exception as e:
            # Fallback to sync execution if queue fails
            try:
                from cara.facades import Log
                Log.warning(f"Queue failed, running synchronously: {str(e)}")
            except:
                pass
            
            # Run synchronously as fallback
            if hasattr(self, 'handle'):
                self.handle()

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

    def get_queue_options(self) -> Dict[str, Any]:
        """Get queue configuration options."""
        options = {
            "queue": self.queue_name,
            "attempts": self._retry_attempts,
        }

        if self._delay is not None:
            options["delay"] = self._delay

        return options

    def failed(self, job_data: Any, error: Exception) -> None:
        """
        Handle job failure.
        Override this in subclasses for custom failure handling.
        """
        # Import here to avoid circular imports
        try:
            from cara.facades import Log

            Log.error(f"{self.__class__.__name__} failed: {str(error)}")
            if hasattr(self, "payload"):
                Log.error(f"Job payload: {self.payload}")
        except ImportError:
            # Silently fail if Log facade not available - this is a framework component
            pass
