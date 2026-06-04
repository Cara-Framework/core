"""
Defines the core contract for notifiable entities in the Cara framework.

Any entity that can receive notifications must implement these methods.
"""

from __future__ import annotations

from typing import Any


class Notifiable:
    """
    A contract for entities that can receive notifications.
    """

    def route_notification_for(self, channel: str) -> Any | None:
        """
        Get the notification routing information for the given channel.

        Args:
            channel: The notification channel (mail, sms, slack, etc.)

        Returns:
            Routing information for the channel
        """
        raise NotImplementedError

    def get_notification_key(self) -> Any:
        """
        Get the key that identifies this notifiable entity.

        Returns:
            The entity's key (usually ID)
        """
        raise NotImplementedError

    def get_notification_type(self) -> str:
        """
        Get the type of this notifiable entity.

        Returns:
            The entity's type/class name
        """
        raise NotImplementedError
