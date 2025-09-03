"""
Queueable Notification base class for Cara Framework.

This class provides DRY functionality specifically for notification operations
that need to be queued for background processing.
"""

from typing import Any, Dict, List

from cara.queues.contracts.BaseQueueable import BaseQueueable


class QueueableNotification(BaseQueueable):
    """
    Base class for queueable notification operations.

    Provides common functionality for all notification classes that need
    to be processed in background queues.
    """

    # Notification-specific queue settings
    default_queue: str = "notifications"
    default_retry_attempts: int = 3

    def __init__(self, notifiable=None, data=None, **kwargs):
        """Initialize queueable notification."""
        self.notifiable = notifiable
        self.notification_data = data or {}
        super().__init__(**kwargs)

    def _initialize_data(self, **kwargs):
        """Notification-specific initialization."""
        # Set notification channels
        self.channels = kwargs.get("channels", self._get_default_channels())

        # Set notification ID if provided
        self.notification_id = kwargs.get("notification_id")

        # Set delay for each channel if needed
        self.channel_delays = kwargs.get("channel_delays", {})

    def _get_default_channels(self) -> List[str]:
        """Get default notification channels."""
        if self.notifiable:
            return self.via(self.notifiable)
        return ["database"]  # Default fallback

    def via_channels(self, channels: List[str]) -> "QueueableNotification":
        """Set specific channels for this notification."""
        self.channels = channels
        return self

    def via_database(self) -> "QueueableNotification":
        """Send only via database channel."""
        return self.via_channels(["database"])

    def via_mail(self) -> "QueueableNotification":
        """Send only via mail channel."""
        return self.via_channels(["mail"])

    def via_sms(self) -> "QueueableNotification":
        """Send only via SMS channel."""
        return self.via_channels(["sms"])

    def with_data(self, data: Dict[str, Any]) -> "QueueableNotification":
        """Add data to notification."""
        self.notification_data.update(data)
        return self

    def delay_for_channel(self, channel: str, seconds: int) -> "QueueableNotification":
        """Set delay for specific channel."""
        self.channel_delays[channel] = seconds
        return self

    def get_queue_options(self) -> Dict[str, Any]:
        """Get notification-specific queue options."""
        options = super().get_queue_options()

        # Add notification-specific options
        options.update(
            {
                "type": "notification",
                "channels": self.channels,
                "channel_delays": self.channel_delays,
                "notification_id": self.notification_id,
            }
        )

        return options

    def handle(self) -> Any:
        """
        Handle notification delivery.
        Sends notification through all configured channels.
        """
        results = {}

        for channel in self.channels:
            try:
                # Apply channel-specific delay if configured
                if channel in self.channel_delays:
                    # Note: In a real implementation, this would schedule
                    # separate jobs for delayed channels
                    pass

                # Send via specific channel
                result = self._send_via_channel(channel)
                results[channel] = {"success": True, "result": result}

            except Exception as e:
                results[channel] = {"success": False, "error": str(e)}
                self._log_channel_error(channel, e)

        return results

    def _send_via_channel(self, channel: str):
        """Send notification via specific channel."""
        method_name = f"to_{channel}"

        if hasattr(self, method_name):
            method = getattr(self, method_name)
            return method(self.notifiable)
        else:
            raise NotImplementedError(
                f"Channel {channel} not implemented for {self.__class__.__name__}"
            )

    def via(self, notifiable) -> List[str]:
        """
        Get notification channels for given notifiable.
        Must be implemented by subclasses.
        """
        raise NotImplementedError("Notification classes must implement via() method")

    def to_database(self, notifiable):
        """
        Build database notification data.
        Override in subclasses if using database channel.
        """
        return {
            "id": self.notification_id,
            "type": self.__class__.__name__,
            "data": self.notification_data,
        }

    def to_mail(self, notifiable):
        """
        Build mail notification.
        Override in subclasses if using mail channel.
        """
        return None

    def to_sms(self, notifiable):
        """
        Build SMS notification.
        Override in subclasses if using SMS channel.
        """
        return None

    def _log_channel_error(self, channel: str, error: Exception):
        """Log channel-specific errors."""
        try:
            from cara.facades import Log

            Log.error(
                f"Notification {self.__class__.__name__} failed for channel {channel}: {str(error)}"
            )
            if self.notifiable:
                Log.error(f"Notifiable: {self.notifiable}")
        except ImportError:
            print(f"Notification failed for channel {channel}: {str(error)}")
