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

    _notification_model = None
    _preference_model = None

    @classmethod
    def set_notification_model(cls, model) -> None:
        """Register the Notification model at app boot (e.g. in a ServiceProvider)."""
        cls._notification_model = model

    @classmethod
    def set_preference_model(cls, model) -> None:
        """Register the UserPreference model at app boot (e.g. in a ServiceProvider)."""
        cls._preference_model = model

    def _get_notification_model(self):
        if self._notification_model is None:
            raise CaraException(
                "Notification model not registered. "
                "Call Notifiable.set_notification_model() in a ServiceProvider."
            )
        return self._notification_model

    def notifications(self) -> list[dict[str, Any]]:
        """Get all notifications for this notifiable entity."""
        try:
            model = self._get_notification_model()
            return list(
                model.where("user_id", self.id)
                .order_by("created_at", "desc")
                .get()
            )
        except Exception as exc:
            import sys

            print(
                f"[cara.notifications] notifications() failed for "
                f"user_id={getattr(self, 'id', '?')}: {exc}",
                file=sys.stderr,
            )
            return []

    def unread_notifications(self) -> list[dict[str, Any]]:
        """Get all unread notifications for this notifiable entity."""
        try:
            model = self._get_notification_model()
            return list(
                model.where("user_id", self.id)
                .where_null("read_at")
                .order_by("created_at", "desc")
                .get()
            )
        except Exception as exc:
            import sys

            print(
                f"[cara.notifications] unread_notifications() failed for "
                f"user_id={getattr(self, 'id', '?')}: {exc}",
                file=sys.stderr,
            )
            return []

    def read_notifications(self) -> list[dict[str, Any]]:
        """Get all read notifications for this notifiable entity."""
        try:
            model = self._get_notification_model()
            return list(
                model.where("user_id", self.id)
                .where_not_null("read_at")
                .order_by("created_at", "desc")
                .get()
            )
        except Exception as exc:
            import sys

            print(
                f"[cara.notifications] read_notifications() failed for "
                f"user_id={getattr(self, 'id', '?')}: {exc}",
                file=sys.stderr,
            )
            return []

    def mark_as_read(self, notification_ids: list[str] | None = None) -> None:
        """Mark notifications as read."""
        import pendulum

        model = self._get_notification_model()
        query = model.where("user_id", self.id)

        if notification_ids:
            query = query.where_in("id", notification_ids)

        query.update({"read_at": pendulum.now("UTC")})

    def mark_as_unread(self, notification_ids: list[str] | None = None) -> None:
        """Mark notifications as unread."""
        model = self._get_notification_model()
        query = model.where("user_id", self.id)

        if notification_ids:
            query = query.where_in("id", notification_ids)

        query.update({"read_at": None})

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

    def notification_preferences(self, notification_type: str) -> list[str]:
        """Get user's preferred channels for a notification type."""
        import json

        if self._preference_model is None:
            return []

        pref = (
            self._preference_model.where("user_id", self.id)
            .where("key", f"notification.{notification_type}")
            .first()
        )

        if pref:
            try:
                channels = json.loads(pref.value)
                if isinstance(channels, list):
                    return channels
            except (json.JSONDecodeError, TypeError):
                pass

        return []
