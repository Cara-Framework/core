"""
Notifiable Mixin for Cara Framework.

This module provides the Notifiable mixin for entities that can receive notifications,
following Laravel-style notification system.
"""

from __future__ import annotations

from typing import Any

from cara.exceptions import CaraException
from cara.notifications.contracts import Notifiable as NotifiableContract


class Notifiable(NotifiableContract):
    """
    Notifiable mixin for entities that can receive notifications.

    This mixin provides methods for sending notifications to entities like users,
    following Laravel's notification pattern.
    """

    def notify(self, notification) -> None:
        """
        Send the given notification.

        Args:
            notification: Notification instance to send
        """
        from cara.facades import Notification as NotificationFacade

        NotificationFacade.send(self, notification)

    def notify_now(self, notification) -> None:
        """
        Send the given notification immediately.

        Args:
            notification: Notification instance to send
        """
        from cara.facades import Notification as NotificationFacade

        NotificationFacade.send_now(self, notification)

    def route_notification_for(self, channel: str) -> Any | None:
        """
        Get the notification routing information for the given channel.

        Args:
            channel: The notification channel (mail, sms, slack, etc.)

        Returns:
            Routing information for the channel
        """
        method_name = f"route_notification_for_{channel}"
        if hasattr(self, method_name):
            return getattr(self, method_name)()
        return None

    def route_notification_for_mail(self) -> str | None:
        """
        Get the email address for mail notifications.

        Returns:
            Email address or None
        """
        if hasattr(self, "email"):
            return self.email
        return None

    def route_notification_for_sms(self) -> str | None:
        """
        Get the phone number for SMS notifications.

        Returns:
            Phone number or None
        """
        if hasattr(self, "phone"):
            return self.phone
        elif hasattr(self, "phone_number"):
            return self.phone_number
        return None

    def route_notification_for_slack(self) -> str | None:
        """
        Get the Slack webhook URL for Slack notifications.

        Returns:
            Slack webhook URL or None
        """
        if hasattr(self, "slack_webhook_url"):
            return self.slack_webhook_url
        return None

    @staticmethod
    def _database_notification_channel():
        """Resolve Cara's conventional polymorphic notification store."""
        try:
            from cara.facades import Notification as NotificationFacade

            channel = NotificationFacade.channel("database")
        except Exception as exc:
            raise CaraException(
                "The database notification channel is not registered."
            ) from exc
        for method in ("get_notifications", "mark_as_read", "mark_as_unread"):
            if not callable(getattr(channel, method, None)):
                raise CaraException(
                    f"The database notification channel does not support {method}()."
                )
        return channel

    def notifications(self) -> list[dict[str, Any]]:
        """Get all notifications for this notifiable entity."""
        return list(self._database_notification_channel().get_notifications(self))

    def unread_notifications(self) -> list[dict[str, Any]]:
        """Get all unread notifications for this notifiable entity."""
        return list(
            self._database_notification_channel().get_notifications(
                self,
                read=False,
            )
        )

    def read_notifications(self) -> list[dict[str, Any]]:
        """Get all read notifications for this notifiable entity."""
        return list(
            self._database_notification_channel().get_notifications(
                self,
                read=True,
            )
        )

    def mark_as_read(self, notification_ids: list[str] | None = None) -> None:
        """Mark notifications as read."""
        self._database_notification_channel().mark_as_read(self, notification_ids)

    def mark_as_unread(self, notification_ids: list[str] | None = None) -> None:
        """Mark notifications as unread."""
        self._database_notification_channel().mark_as_unread(self, notification_ids)

    def get_notification_key(self) -> Any:
        """
        Get the key that identifies this notifiable entity.

        Returns:
            The entity's key (usually ID)
        """
        if hasattr(self, "id"):
            return self.id
        elif hasattr(self, "pk"):
            return self.pk
        raise CaraException(
            "Notifiable entities must expose a stable id or pk notification key."
        )

    def get_notification_type(self) -> str:
        """
        Get the type of this notifiable entity.

        Returns:
            The entity's type/class name
        """
        return self.__class__.__name__
