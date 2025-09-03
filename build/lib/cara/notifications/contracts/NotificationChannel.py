"""
Defines the core contract for notification channels in the Cara framework.

Any notification channel (mail, database, slack, etc.) must implement these methods. This ensures consistent behavior
(send) across channels.
"""

from typing import Any


class NotificationChannel:
    """
    A simple contract for notification channel operations.

    Methods:
    - send(notifiable, notification)
    """

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
