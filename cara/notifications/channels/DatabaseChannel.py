"""
Database Channel for Cara Notifications.

This module provides database notification channel functionality,
storing notifications in the database for later retrieval.
"""

from __future__ import annotations

from typing import Any

import pendulum

from cara.exceptions import ConfigurationException
from cara.notifications.channels.BaseChannel import BaseChannel


class DatabaseChannel(BaseChannel):
    """
    Database channel for storing notifications in database.

    This channel stores notifications in a database table for later retrieval,
    useful for in-app notifications and notification history.
    """

    channel_name = "database"

    def __init__(self, database_manager, table_name: str = "notifications"):
        """
        Initialize database channel.

        Args:
            database_manager: Database manager instance (REQUIRED)
            table_name: Name of the notifications table
        """
        if database_manager is None:
            raise ConfigurationException(
                "Database manager is required for DatabaseChannel"
            )

        self.database_manager = database_manager
        self.table_name = table_name

    def send(self, notifiable, notification) -> bool:
        """
        Store the notification in database.

        Args:
            notifiable: The notifiable entity
            notification: The notification instance

        Returns:
            True if stored successfully, False otherwise
        """
        data = notification.to_database(notifiable)
        if data is None:
            data = notification.to_array(notifiable)
        if data is None:
            data = {}

        notification_type = notification.__class__.__name__

        # Cara owns only the conventional polymorphic database-notification
        # shape. Products with a different schema register their own channel;
        # app-specific tenant/user/status columns must never leak into the
        # framework.
        now = pendulum.now("UTC")
        record = {
            "type": notification_type,
            "notifiable_type": self._get_notifiable_type(notifiable),
            "notifiable_id": self._get_notifiable_id(notifiable),
            "data": self._serialize_data(data if isinstance(data, dict) else {}),
            "read_at": None,
            "created_at": now,
            "updated_at": now,
        }

        # Add notification ID if set
        if notification.get_id():
            record["id"] = notification.get_id()

        return self._store_notification(record)

    def _get_notifiable_id(self, notifiable) -> Any:
        """
        Get the notifiable entity ID.

        Args:
            notifiable: The notifiable entity

        Returns:
            Entity ID
        """
        if hasattr(notifiable, "get_notification_key"):
            return notifiable.get_notification_key()
        elif hasattr(notifiable, "id"):
            return notifiable.id
        elif hasattr(notifiable, "pk"):
            return notifiable.pk
        else:
            return id(notifiable)

    def _serialize_data(self, data: dict[str, Any]) -> str:
        """
        Serialize notification data.

        Args:
            data: Data to serialize

        Returns:
            Serialized data string
        """
        import json

        try:
            return json.dumps(data, default=str)
        except Exception:
            return str(data)

    def _get_notifiable_type(self, notifiable) -> str:
        """Get the framework-level polymorphic recipient type."""
        if hasattr(notifiable, "get_notification_type"):
            return str(notifiable.get_notification_type())
        return notifiable.__class__.__name__

    def _store_notification(self, record: dict[str, Any]) -> bool:
        """
        Store notification record in database.

        Args:
            record: Notification record to store

        Returns:
            True if stored successfully, False otherwise
        """
        # Use database manager to store notification
        query_builder = self.database_manager.table(self.table_name)
        query_builder.create(record)
        return True

    def mark_as_read(self, notifiable, notification_ids: list | None = None) -> bool:
        """
        Mark notifications as read.

        Args:
            notifiable: The notifiable entity
            notification_ids: List of notification IDs to mark as read

        Returns:
            True if updated successfully, False otherwise
        """
        query = (
            self.database_manager.table(self.table_name)
            .where("notifiable_type", self._get_notifiable_type(notifiable))
            .where("notifiable_id", self._get_notifiable_id(notifiable))
        )

        if notification_ids:
            query = query.where_in("id", notification_ids)

        query.update({"read_at": pendulum.now("UTC")})
        return True

    def get_notifications(self, notifiable, read: bool | None = None) -> list:
        """
        Get notifications for a notifiable entity.

        Args:
            notifiable: The notifiable entity
            read: True for read notifications, False for unread, None for all

        Returns:
            List of notifications
        """
        query = (
            self.database_manager.table(self.table_name)
            .where("notifiable_type", self._get_notifiable_type(notifiable))
            .where("notifiable_id", self._get_notifiable_id(notifiable))
        )

        if read is True:
            query = query.where_not_null("read_at")
        elif read is False:
            query = query.where_null("read_at")

        return query.order_by("created_at", "desc").get()

    def mark_as_unread(self, notifiable, notification_ids: list | None = None) -> bool:
        """Mark notifications as unread for one polymorphic recipient."""
        query = (
            self.database_manager.table(self.table_name)
            .where("notifiable_type", self._get_notifiable_type(notifiable))
            .where("notifiable_id", self._get_notifiable_id(notifiable))
        )
        if notification_ids:
            query = query.where_in("id", notification_ids)
        query.update({"read_at": None})
        return True
