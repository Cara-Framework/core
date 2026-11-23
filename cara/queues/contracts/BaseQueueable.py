"""
Base class for all queueable operations in Cara Framework.

This class eliminates code repetition across Mail, Notification, Job
and other queueable classes by providing common functionality.
"""

from __future__ import annotations

import contextlib
import inspect
from typing import Any

from cara.exceptions import InvalidArgumentException
from cara.facades import Log

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
        self.queue_name = self.default_queue
        self._delay = self.default_delay

    def _initialize_data(self, *args, **kwargs):
        """Override this in subclasses for custom initialization."""
        pass

    @property
    def queue_name(self) -> str | None:
        """Get the queue name for this job."""
        return self._queue_name

    @queue_name.setter
    def queue_name(self, name: str | None):
        """Set the queue name for this job.

        Mirrored onto ``queue``, the attribute the queue driver and
        ``PendingDispatch`` actually read: kept apart, ``default_queue`` and
        ``on_queue()`` never reached the broker and every such job was
        refused for declaring no canonical queue.
        """
        self._queue_name = name
        self.queue = name

    @classmethod
    def _declared_max_attempts(cls) -> int:
        """The retry budget this class declares: the only one a worker sees.

        A class attribute ``max_attempts = n`` shadows the property below and
        wins; otherwise ``default_retry_attempts``.
        """
        declared = inspect.getattr_static(cls, "max_attempts")
        if isinstance(declared, property):
            return cls.default_retry_attempts
        return declared

    @property
    def max_attempts(self) -> int:
        """The delivery budget the worker reads (``JobProcessor``).

        Read from the class, never from instance state: the worker rebuilds
        the job from its signed constructor parameters and the envelope
        carries no per-dispatch budget, so an instance value could not reach
        it. (``default_retry_attempts`` used to be written to an attribute
        nothing read, so ``SendMailableJob``'s declared 5 ran as 3.)
        """
        return type(self)._declared_max_attempts()

    @max_attempts.setter
    def max_attempts(self, count: int) -> None:
        self._refuse_instance_attempts(count)

    def attempts(self, count: int) -> BaseQueueable:
        """Confirm the class's retry budget; any other value is refused.

        A per-instance budget never crosses the broker (see
        :attr:`max_attempts`), and carrying one would need a new signed
        envelope field that workers of the previous release would refuse
        during a rolling deploy. So the budget is declared on the class, and
        a value that differs from it raises here instead of being dropped
        silently on the worker.
        """
        self._refuse_instance_attempts(count)
        self._chained = True
        return self

    def _refuse_instance_attempts(self, count: int) -> None:
        declared = type(self)._declared_max_attempts()
        if count == declared and not isinstance(count, bool):
            return
        name = type(self).__name__
        raise InvalidArgumentException(
            f"{name}: a per-dispatch attempts({count!r}) cannot take effect — "
            "the worker rebuilds the job from its constructor arguments and "
            f"reads the class's budget ({declared}). Declare "
            f"`max_attempts = {count!r}` (or `default_retry_attempts`) on "
            f"{name} instead."
        )

    def delay(self, seconds: int) -> BaseQueueable:
        """Delay the job execution by specified seconds.

        ``PendingDispatch(job)`` starts from this value (and
        ``default_delay``); an explicit ``PendingDispatch.delay()`` wins.
        """
        self._delay = seconds
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
            "attempts": self.max_attempts,
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
        with contextlib.suppress(ImportError, RuntimeError):
            Log.error("%s failed: %s", self.__class__.__name__, str(error))
