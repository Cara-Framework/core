"""
Database Channel for Cara Notifications.

This module provides database notification channel functionality,
storing notifications in the database for later retrieval.
"""

import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from cara.notifications.channels import BaseChannel


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
            raise ValueError("Database manager is required for DatabaseChannel")

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
        # Check if this is a wrapper notification (from Laravel-style routing)
        if hasattr(notification, "data") and hasattr(notification, "original"):
            # Use the wrapper's data directly
            data = notification.data
            notification_type = notification.original.__class__.__name__
            original_notification = notification.original
        else:
            # Regular notification - try to get database representation
            data = None
            if hasattr(notification, "to_database"):
                data = notification.to_database(notifiable)

            if data is None and hasattr(notification, "to_array"):
                # Fall back to array representation
                data = notification.to_array(notifiable)

            if data is None:
                # Fallback to empty dict
                data = {}

            notification_type = notification.__class__.__name__
            original_notification = notification

        # Prepare notification record
        record = {
            "id": str(uuid.uuid4()),
            "type": notification_type,
            "notifiable_type": notifiable.get_notification_type()
            if hasattr(notifiable, "get_notification_type")
            else notifiable.__class__.__name__,
            "notifiable_id": self._get_notifiable_id(notifiable),
            "data": self._serialize_data(data),
            "read_at": None,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }

        # Add notification ID if set
        if hasattr(original_notification, "get_id") and original_notification.get_id():
            record["id"] = original_notification.get_id()

        # Store in database
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

    def _serialize_data(self, data: Dict[str, Any]) -> str:
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

    def _store_notification(self, record: Dict[str, Any]) -> bool:
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

    def mark_as_read(self, notifiable, notification_ids: list = None) -> bool:
        """
        Mark notifications as read.

        Args:
            notifiable: The notifiable entity
            notification_ids: List of notification IDs to mark as read

        Returns:
            True if updated successfully, False otherwise
        """
        notifiable_id = self._get_notifiable_id(notifiable)
        notifiable_type = (
            notifiable.get_notification_type()
            if hasattr(notifiable, "get_notification_type")
            else notifiable.__class__.__name__
        )

        query = (
            self.database_manager.table(self.table_name)
            .where("notifiable_type", notifiable_type)
            .where("notifiable_id", notifiable_id)
        )

        if notification_ids:
            query = query.where_in("id", notification_ids)

        query.update({"read_at": datetime.utcnow()})
        return True

    def get_notifications(self, notifiable, read: Optional[bool] = None) -> list:
        """
        Get notifications for a notifiable entity.

        Args:
            notifiable: The notifiable entity
            read: True for read notifications, False for unread, None for all

        Returns:
            List of notifications
        """
        notifiable_id = self._get_notifiable_id(notifiable)
        notifiable_type = (
            notifiable.get_notification_type()
            if hasattr(notifiable, "get_notification_type")
            else notifiable.__class__.__name__
        )

        query = (
            self.database_manager.table(self.table_name)
            .where("notifiable_type", notifiable_type)
            .where("notifiable_id", notifiable_id)
        )

        if read is True:
            query = query.where_not_null("read_at")
        elif read is False:
            query = query.where_null("read_at")

        return query.order_by("created_at", "desc").get()
