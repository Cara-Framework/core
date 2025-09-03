"""
Base Notification Class for Cara Framework.

This module provides the base Notification class for creating and sending notifications
through multiple channels, following Laravel-style API with Cara framework conventions.
Includes automatic serialization support for queue jobs.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from cara.queues.contracts import SerializesModels


class BaseNotification(ABC, SerializesModels):
    """
    Base class for creating notification objects.

    This class provides Laravel-style API for building notifications with support for
    multiple channels like mail, database, SMS, Slack, etc.

    Automatically includes SerializesModels mixin for proper serialization
    when used with ShouldQueue.
    """

    def __init__(self):
        """Initialize notification with default values."""
        super().__init__()
        self._id: Optional[str] = None
        self._data: Dict[str, Any] = {}
        self._delay: Optional[int] = None
        self._queue: Optional[str] = None

    @abstractmethod
    def via(self, notifiable) -> List[str]:
        """
        Get the notification's delivery channels.

        Args:
            notifiable: The notifiable entity receiving the notification

        Returns:
            List of channel names
        """
        pass

    def to_mail(self, notifiable) -> Optional[Any]:
        """
        Get the mail representation of the notification.

        Args:
            notifiable: The notifiable entity

        Returns:
            Mail representation or None
        """
        return None

    def to_database(self, notifiable) -> Optional[Dict[str, Any]]:
        """
        Get the database representation of the notification.

        Args:
            notifiable: The notifiable entity

        Returns:
            Database data dictionary or None
        """
        return None

    def to_array(self, notifiable) -> Dict[str, Any]:
        """
        Get the array representation of the notification.

        Args:
            notifiable: The notifiable entity

        Returns:
            Array representation
        """
        return self._data

    def to_slack(self, notifiable) -> Optional[Dict[str, Any]]:
        """
        Get the Slack representation of the notification.

        Args:
            notifiable: The notifiable entity

        Returns:
            Slack data dictionary or None
        """
        return None

    def to_log(self, notifiable) -> Optional[Dict[str, Any]]:
        """
        Get the log representation of the notification.

        Args:
            notifiable: The notifiable entity

        Returns:
            Log data dictionary or None
        """
        return None

    def id(self, notification_id: str) -> "BaseNotification":
        """
        Set the notification ID.

        Args:
            notification_id: Unique notification identifier

        Returns:
            Self for method chaining
        """
        self._id = notification_id
        return self

    def delay(self, seconds: int) -> "BaseNotification":
        """
        Set the notification delay.

        Args:
            seconds: Delay in seconds

        Returns:
            Self for method chaining
        """
        self._delay = seconds
        return self

    def on_queue(self, queue: str) -> "BaseNotification":
        """
        Set the queue for the notification.

        Args:
            queue: Queue name

        Returns:
            Self for method chaining
        """
        self._queue = queue
        return self

    def with_data(self, data: Dict[str, Any]) -> "BaseNotification":
        """
        Set additional data for the notification.

        Args:
            data: Additional data dictionary

        Returns:
            Self for method chaining
        """
        self._data.update(data)
        return self

    def get_id(self) -> Optional[str]:
        """Get the notification ID."""
        return self._id

    def get_data(self) -> Dict[str, Any]:
        """Get the notification data."""
        return self._data

    def get_delay(self) -> Optional[int]:
        """Get the notification delay."""
        return self._delay

    def get_queue(self) -> Optional[str]:
        """Get the notification queue."""
        return self._queue

    def should_send(self, notifiable, channel: str) -> bool:
        """
        Determine if the notification should be sent.

        Args:
            notifiable: The notifiable entity
            channel: The channel being used

        Returns:
            True if notification should be sent
        """
        return True

    def __str__(self) -> str:
        """String representation for debugging."""
        return f"<Notification {self.__class__.__name__} id={self._id}>"
