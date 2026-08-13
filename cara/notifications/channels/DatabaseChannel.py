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
from cara.support import json_dumps


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
        if database_manager is None or not callable(
            getattr(database_manager, "table", None)
        ):
            raise ConfigurationException(
                "DatabaseChannel requires a database manager with table()."
            )
        if not isinstance(table_name, str) or not table_name.strip():
            raise ConfigurationException(
                "DatabaseChannel table_name must be a non-empty string."
            )

        self.database_manager = database_manager
        self.table_name = table_name.strip()

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
        if not isinstance(data, dict):
            raise ConfigurationException(
                "Database notifications must render a dictionary payload."
            )

        notification_type = notification.__class__.__name__

        # Cara owns only the conventional polymorphic database-notification
        # shape. Products with a different schema register their own channel;
        # app-specific tenant/user/status columns must never leak into the
        # framework.
        now = pendulum.now("UTC")
        record = {
            "type": notification_type,
            "notifiable_type": self.notifiable_type(notifiable),
            "notifiable_id": self.notifiable_id(notifiable),
            "data": self._serialize_data(data),
            "read_at": None,
            "created_at": now,
            "updated_at": now,
        }

        # Add notification ID if set
        if notification.get_id():
            record["id"] = notification.get_id()

        return self._store_notification(record)

    def _serialize_data(self, data: dict[str, Any]) -> str:
        """
        Serialize notification data.

        Args:
            data: Data to serialize

        Returns:
            Serialized data string
        """

        return json_dumps(data)

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
            .where("notifiable_type", self.notifiable_type(notifiable))
            .where("notifiable_id", self.notifiable_id(notifiable))
        )

        if notification_ids is not None:
            self._validate_notification_ids(notification_ids)
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
            .where("notifiable_type", self.notifiable_type(notifiable))
            .where("notifiable_id", self.notifiable_id(notifiable))
        )

        if read is not None and not isinstance(read, bool):
            raise ConfigurationException("Notification read filter must be bool or None.")
        if read is True:
            query = query.where_not_null("read_at")
        elif read is False:
            query = query.where_null("read_at")

        return query.order_by("created_at", "desc").get()

    def mark_as_unread(self, notifiable, notification_ids: list | None = None) -> bool:
        """Mark notifications as unread for one polymorphic recipient."""
        query = (
            self.database_manager.table(self.table_name)
            .where("notifiable_type", self.notifiable_type(notifiable))
            .where("notifiable_id", self.notifiable_id(notifiable))
        )
        if notification_ids is not None:
            self._validate_notification_ids(notification_ids)
            query = query.where_in("id", notification_ids)
        query.update({"read_at": None})
        return True

    @staticmethod
    def _validate_notification_ids(notification_ids: list) -> None:
        if not isinstance(notification_ids, list) or not notification_ids:
            raise ConfigurationException(
                "Notification id filters must be non-empty lists."
            )
        if any(
            isinstance(value, bool)
            or not isinstance(value, (str, int))
            or (isinstance(value, str) and not value.strip())
            for value in notification_ids
        ):
            raise ConfigurationException(
                "Notification id filters must contain non-empty string or integer ids."
            )
        if len(set(notification_ids)) != len(notification_ids):
            raise ConfigurationException(
                "Notification id filters must not contain duplicate ids."
            )
