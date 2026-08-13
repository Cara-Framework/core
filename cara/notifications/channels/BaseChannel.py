"""
Base Notification Channel for the Cara framework.

This module provides an abstract base class for notification channels,
implementing the NotificationChannel.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from cara.exceptions import InvalidArgumentException
from cara.notifications.contracts import NotificationChannel


class BaseChannel(NotificationChannel):
    """
    Abstract base class for notification channels.

    Subclasses must override the send method.
    """

    channel_name: str = ""

    @staticmethod
    def notifiable_id(notifiable: Any) -> Any:
        """Return a stable recipient key; ephemeral object identity is invalid."""
        key_getter = getattr(notifiable, "get_notification_key", None)
        if callable(key_getter):
            key = key_getter()
        elif hasattr(notifiable, "id"):
            key = notifiable.id
        elif hasattr(notifiable, "pk"):
            key = notifiable.pk
        else:
            raise InvalidArgumentException(
                "Notification recipients must expose get_notification_key(), id, or pk."
            )
        if (
            isinstance(key, bool)
            or not isinstance(key, (str, int, UUID))
            or (isinstance(key, str) and not key.strip())
        ):
            raise InvalidArgumentException(
                "Notification recipient keys must be non-empty string, integer, or UUID "
                "identifiers."
            )
        return key.strip() if isinstance(key, str) else key

    @staticmethod
    def notifiable_type(notifiable: Any) -> str:
        """Return the explicit polymorphic type or the concrete class name."""
        type_getter = getattr(notifiable, "get_notification_type", None)
        value = type_getter() if callable(type_getter) else type(notifiable).__name__
        if not isinstance(value, str) or not value.strip():
            raise InvalidArgumentException(
                "Notification recipient types must be non-empty strings."
            )
        return value.strip()

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
