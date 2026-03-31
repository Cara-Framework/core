"""
Base Notification Channel for the Cara framework.

This module provides an abstract base class for notification channels,
implementing the NotificationChannel.
"""

from typing import Any

from cara.notifications.contracts import NotificationChannel


class BaseChannel(NotificationChannel):
    """
    Abstract base class for notification channels.

    Subclasses must override the send method.
    """

    channel_name: str = ""

    def send(self, notifiable: Any, notification: Any) -> bool:
        """
        Send the notification through this channel.

        Args:
            notifiable: The entity to notify
            notification: The notification to send

        Returns:
            True if sent successfully, False otherwise
        """
        raise NotImplementedError
