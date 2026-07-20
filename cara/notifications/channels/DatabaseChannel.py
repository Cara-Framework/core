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
            raise ConfigurationException("Database manager is required for DatabaseChannel")

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

        # Map to the application's ``notification`` table schema.
        # The table uses ``user_id`` (not ``notifiable_id``) and stores
        # structured fields (``title``, ``body``, ``channel``) instead
        # of a single ``data`` JSON blob. The notification's own
        # ``to_database()`` dict is AUTHORITATIVE: app-specific columns it
        # carries (``tenant_id``, ``type`` from the app's registry, …)
        # pass through and override these defaults, so the channel works
        # against tenant-scoped schemas instead of fighting them.
        now = pendulum.now("UTC")
        record = {
            "user_id": self._get_notifiable_id(notifiable),
            "type": notification_type,
            "channel": "database",
            "title": data.get("title", notification_type)
            if isinstance(data, dict)
            else notification_type,
            "body": data.get("body", "") if isinstance(data, dict) else "",
            "action_url": data.get("action_url") if isinstance(data, dict) else None,
            "status": "delivered",
            "sent_at": now,
            "delivered_at": now,
            "read_at": None,
            "metadata": self._serialize_data(data),
            "created_at": now,
            "updated_at": now,
        }
        if isinstance(data, dict):
            for key, value in data.items():
                if key in ("title", "body", "action_url"):
                    continue  # already mapped above
                record[key] = value

        # Add notification ID if set
        if hasattr(original_notification, "get_id") and original_notification.get_id():
            record["id"] = original_notification.get_id()

        # Prefer the app-registered notification MODEL when one exists —
        # model creation runs the app's hooks (public ids, casts,
        # tenant scoping) that a raw table insert would silently skip.
        from cara.notifications.Notifiable import Notifiable

        model = Notifiable._notification_model
        if model is not None:
            payload = {
                key: value
                for key, value in record.items()
                if key not in ("id", "created_at", "updated_at", "metadata")
            }
            payload["metadata"] = data if isinstance(data, dict) else {"value": str(data)}
            model.create(payload)
            return True

        # Store in database (raw-table fallback for model-less apps)
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
        user_id = self._get_notifiable_id(notifiable)

        query = self.database_manager.table(self.table_name).where("user_id", user_id)

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
        user_id = self._get_notifiable_id(notifiable)

        query = self.database_manager.table(self.table_name).where("user_id", user_id)

        if read is True:
            query = query.where_not_null("read_at")
        elif read is False:
            query = query.where_null("read_at")

        return query.order_by("created_at", "desc").get()
