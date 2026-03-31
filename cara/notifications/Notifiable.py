"""
Notifiable Mixin for Cara Framework.

This module provides the Notifiable mixin for entities that can receive notifications,
following Laravel-style notification system.
"""

from typing import Any, Dict, List, Optional

from cara.notifications.contracts import Notifiable


class Notifiable(Notifiable):
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

    def route_notification_for(self, channel: str) -> Optional[Any]:
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

    def route_notification_for_mail(self) -> Optional[str]:
        """
        Get the email address for mail notifications.

        Returns:
            Email address or None
        """
        if hasattr(self, "email"):
            return self.email
        return None

    def route_notification_for_sms(self) -> Optional[str]:
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

    def route_notification_for_slack(self) -> Optional[str]:
        """
        Get the Slack webhook URL for Slack notifications.

        Returns:
            Slack webhook URL or None
        """
        if hasattr(self, "slack_webhook_url"):
            return self.slack_webhook_url
        return None

    def notifications(self) -> List[Dict[str, Any]]:
        """
        Get all notifications for this notifiable entity.

        Returns:
            List of notifications
        """
        # This would typically query a database
        # For now, return empty list
        return []

    def unread_notifications(self) -> List[Dict[str, Any]]:
        """
        Get all unread notifications for this notifiable entity.

        Returns:
            List of unread notifications
        """
        # This would typically query a database with read_at = None
        # For now, return empty list
        return []

    def read_notifications(self) -> List[Dict[str, Any]]:
        """
        Get all read notifications for this notifiable entity.

        Returns:
            List of read notifications
        """
        # This would typically query a database with read_at != None
        # For now, return empty list
        return []

    def mark_as_read(self, notification_ids: List[str] = None) -> None:
        """
        Mark notifications as read.

        Args:
            notification_ids: List of notification IDs to mark as read.
                            If None, marks all as read.
        """
        # This would typically update the database
        pass

    def mark_as_unread(self, notification_ids: List[str] = None) -> None:
        """
        Mark notifications as unread.

        Args:
            notification_ids: List of notification IDs to mark as unread.
                            If None, marks all as unread.
        """
        # This would typically update the database
        pass

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
        else:
            return id(self)

    def get_notification_type(self) -> str:
        """
        Get the type of this notifiable entity.

        Returns:
            The entity's type/class name
        """
        return self.__class__.__name__
