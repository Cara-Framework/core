"""
Queueable Mail base class for Cara Framework.

This class provides DRY functionality specifically for mail operations
that need to be queued for background processing.
"""

from typing import Any, Dict

from cara.queues.contracts.BaseQueueable import BaseQueueable


class QueueableMail(BaseQueueable):
    """
    Base class for queueable mail operations.

    Provides common functionality for all mail classes that need
    to be processed in background queues.
    """

    # Mail-specific queue settings
    default_queue: str = "emails"
    default_retry_attempts: int = 5  # Emails need more retries

    def __init__(self, user=None, **kwargs):
        """Initialize queueable mail with user."""
        self.user = user
        super().__init__(**kwargs)

    def _initialize_data(self, **kwargs):
        """Mail-specific initialization."""
        # Set mail priority based on type
        self.priority = kwargs.get("priority", "normal")

        # Setup mail delivery options
        self.delivery_options = kwargs.get("delivery_options", {})

    def high_priority(self) -> "QueueableMail":
        """Mark this mail as high priority."""
        self.priority = "high"
        self.queue_name = "emails-priority"
        return self

    def low_priority(self) -> "QueueableMail":
        """Mark this mail as low priority."""
        self.priority = "low"
        self.queue_name = "emails-low"
        return self

    def delay_minutes(self, minutes: int) -> "QueueableMail":
        """Delay mail delivery by specified minutes."""
        return self.delay(minutes * 60)

    def delay_hours(self, hours: int) -> "QueueableMail":
        """Delay mail delivery by specified hours."""
        return self.delay(hours * 3600)

    def get_queue_options(self) -> Dict[str, Any]:
        """Get mail-specific queue options."""
        options = super().get_queue_options()

        # Add mail-specific options
        options.update(
            {
                "priority": self.priority,
                "type": "mail",
                "delivery_options": self.delivery_options,
            }
        )

        return options

    def handle(self) -> Any:
        """
        Handle mail delivery.
        This calls the build() method and sends the mail.
        """
        try:
            # Build the mail content
            mail_content = self.build()

            # Send the mail (implementation depends on mail driver)
            return self._send_mail(mail_content)

        except Exception as e:
            # Log the error and re-raise for queue retry handling
            self._log_mail_error(e)
            raise

    def build(self):
        """
        Build the mail content.
        Must be implemented by subclasses.
        """
        raise NotImplementedError("Mail classes must implement build() method")

    def _send_mail(self, mail_content):
        """
        Send the actual mail.
        This will be handled by the mail system.
        """
        # This will be implemented by the mail facade/driver
        from cara.facades import Mail

        return Mail.send(mail_content)

    def _log_mail_error(self, error: Exception):
        """Log mail-specific errors."""
        try:
            from cara.facades import Log

            Log.error(f"Mail delivery failed for {self.__class__.__name__}: {str(error)}")
            if self.user:
                Log.error(f"Recipient: {getattr(self.user, 'email', 'unknown')}")
        except ImportError:
            print(f"Mail delivery failed: {str(error)}")
