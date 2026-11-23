"""
Base Notification Class for Cara Framework.

This module provides the base Notification class for creating and sending notifications
through multiple channels, following Laravel-style API with Cara framework conventions.
Includes automatic serialization support for queue jobs.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from cara.exceptions import InvalidArgumentException
from cara.queues import QueuePayload
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
        self._id: str | None = None
        self._data: dict[str, Any] = {}
        self._delay: int | None = None
        self._queue: str | None = None

    @abstractmethod
    def via(self, notifiable) -> list[str]:
        """
        Get the notification's delivery channels.

        Args:
            notifiable: The notifiable entity receiving the notification

        Returns:
            List of channel names
        """
        pass

    def to_mail(self, notifiable) -> Any | None:
        """
        Get the mail representation of the notification.

        Args:
            notifiable: The notifiable entity

        Returns:
            Mail representation or None
        """
        return None

    def to_database(self, notifiable) -> dict[str, Any] | None:
        """
        Get the database representation of the notification.

        Args:
            notifiable: The notifiable entity

        Returns:
            Database data dictionary or None
        """
        return None

    def to_array(self, notifiable) -> dict[str, Any]:
        """
        Get the array representation of the notification.

        Args:
            notifiable: The notifiable entity

        Returns:
            Array representation
        """
        return self._data

    def to_slack(self, notifiable) -> dict[str, Any] | None:
        """
        Get the Slack representation of the notification.

        Args:
            notifiable: The notifiable entity

        Returns:
            Slack data dictionary or None
        """
        return None

    def to_log(self, notifiable) -> dict[str, Any] | None:
        """
        Get the log representation of the notification.

        Args:
            notifiable: The notifiable entity

        Returns:
            Log data dictionary or None
        """
        return None

    def id(self, notification_id: str) -> BaseNotification:
        """
        Set the notification ID.

        Args:
            notification_id: Unique notification identifier

        Returns:
            Self for method chaining
        """
        if not isinstance(notification_id, str) or not notification_id.strip():
            raise InvalidArgumentException("Notification id must be a non-empty string.")
        self._id = notification_id.strip()
        return self

    def delay(self, seconds: int) -> BaseNotification:
        """
        Set the notification delay.

        Args:
            seconds: Delay in seconds

        Returns:
            Self for method chaining
        """
        if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds <= 0:
            raise InvalidArgumentException(
                "Notification delay must be a positive integer number of seconds."
            )
        self._delay = seconds
        return self

    def on_queue(self, queue: str) -> BaseNotification:
        """
        Set the queue for the notification.

        Args:
            queue: Queue name

        Returns:
            Self for method chaining
        """
        if not isinstance(queue, str) or not queue.strip():
            raise InvalidArgumentException(
                "Notification queue must be a non-empty string."
            )
        self._queue = queue.strip()
        return self

    def with_data(self, data: dict[str, Any]) -> BaseNotification:
        """
        Set additional data for the notification.

        Args:
            data: Additional data dictionary

        Returns:
            Self for method chaining
        """
        if not isinstance(data, dict):
            raise InvalidArgumentException("Notification data must be a dictionary.")
        self._data.update(data)
        return self

    def get_id(self) -> str | None:
        """Get the notification ID."""
        return self._id

    def get_data(self) -> dict[str, Any]:
        """Get the notification data."""
        return self._data

    def get_delay(self) -> int | None:
        """Get the notification delay."""
        return self._delay

    def get_queue(self) -> str | None:
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

    #: State ``BaseNotification.__init__`` owns. ``_delay`` and ``_queue``
    #: are dispatch routing, applied before the job is queued.
    _BASE_STATE = frozenset({"_id", "_data", "_delay", "_queue"})

    def to_queue_payload(self) -> dict[str, Any]:
        """This notification as plain data for a queued send.

        The default carries the public attributes (the constructor state a
        subclass keeps, such as ``self.order_id``) plus the id and data. A
        model or other live object is refused with its field named — store
        its id. A private attribute of the subclass is refused too: the
        default rebuild could not restore it, and finding that out on the
        worker would be too late. Override this and
        :meth:`from_queue_payload` together for anything richer.
        """
        attributes: dict[str, Any] = {}
        for name, value in vars(self).items():
            if name in self._BASE_STATE:
                continue
            if name.startswith("_"):
                raise InvalidArgumentException(
                    f"notification_data.{name}: {type(self).__name__} keeps private "
                    "state the default queue payload cannot restore. Override "
                    "to_queue_payload()/from_queue_payload()."
                )
            attributes[name] = value
        return QueuePayload.require_plain(
            {"attributes": attributes, "id": self._id, "data": self._data},
            "notification_data",
        )

    @classmethod
    def from_queue_payload(cls, data: dict[str, Any]) -> BaseNotification:
        """Rebuild the notification :meth:`to_queue_payload` produced.

        The subclass constructor is NOT called — its arguments were never
        queued; its resulting public state was. Base state is initialised by
        ``BaseNotification.__init__`` and the queued attributes are restored
        on top, which is the inverse of the default payload.
        """
        if not isinstance(data, dict):
            raise InvalidArgumentException("Queued notification data must be an object.")
        notification = cls.__new__(cls)
        BaseNotification.__init__(notification)
        for name, value in dict(data.get("attributes") or {}).items():
            if (
                not isinstance(name, str)
                or not name.isidentifier()
                or name.startswith("_")
            ):
                raise InvalidArgumentException(
                    f"Queued notification attribute {name!r} is not a public name."
                )
            setattr(notification, name, value)
        if data.get("id") is not None:
            notification.id(data["id"])
        notification._data = dict(data.get("data") or {})
        return notification

    def __str__(self) -> str:
        """String representation for debugging."""
        return f"<Notification {self.__class__.__name__} id={self._id}>"
