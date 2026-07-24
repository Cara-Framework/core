"""
Central Notification Manager for the Cara framework.

This module provides the Notification class, which manages multiple notification channels and delegates
notification operations to the appropriate channel instances.
"""

from __future__ import annotations

from cara.exceptions import DriverNotRegisteredException, InvalidArgumentException
from cara.facades import Queue
from cara.notifications.contracts import NotificationChannel
from cara.queues.contracts import ShouldQueue


class Notification:
    """
    Central notification manager. Delegates notification sending to registered channel instances.

    Each notification declares its channels through ``via(notifiable)``.
    """

    def __init__(self):
        """Initialize an empty channel registry."""
        self._channels: dict[str, NotificationChannel] = {}

    def add_channel(self, name: str, channel: NotificationChannel) -> None:
        """Register a channel instance under `name`."""
        name = str(name).strip()
        if not name:
            raise InvalidArgumentException(
                "Notification channel name must be a non-empty string."
            )
        if not callable(getattr(channel, "send", None)):
            raise InvalidArgumentException(
                f"Notification channel {name!r} must implement send()."
            )
        if name in self._channels:
            raise InvalidArgumentException(
                f"Notification channel {name!r} is already registered."
            )
        self._channels[name] = channel

    def channel(self, channel_name: str) -> NotificationChannel:
        """
        Return the named channel.

        Raises DriverNotRegisteredException if missing.
        """
        channel = self._channels.get(channel_name)
        if not channel:
            raise DriverNotRegisteredException(
                f"Notification channel '{channel_name}' is not registered."
            )
        return channel

    def send(self, notifiable, notification) -> bool:
        """
        Send a notification to a notifiable entity.

        Laravel-style: If notification implements ShouldQueue, dispatch to queue.
        Otherwise, send immediately.

        Args:
            notifiable: The entity to notify
            notification: The notification to send

        Returns:
            True if sent/queued successfully, False otherwise
        """
        # Laravel-style queue check: If notification implements ShouldQueue, queue it
        if self._should_queue(notification):
            return self._queue_notification(notifiable, notification)

        # Otherwise send immediately
        return self._send_now(notifiable, notification)

    def _should_queue(self, notification) -> bool:
        """
        Check if notification should be queued (Laravel-style ShouldQueue interface).

        Args:
            notification: The notification instance

        Returns:
            True if notification implements ShouldQueue, False otherwise
        """
        return isinstance(notification, ShouldQueue)

    def _queue_notification(self, notifiable, notification) -> bool:
        """
        Queue a notification for background processing.

        Args:
            notifiable: The entity to notify
            notification: The notification to queue

        Returns:
            True if queued successfully, False otherwise
        """
        from cara.notifications.jobs import SendNotificationJob

        job = SendNotificationJob(notifiable, notification)

        # Honor the notification's chainable queue/delay overrides
        # (``on_queue()`` / ``delay()``) — Laravel parity: they must
        # survive into the dispatched job, not just sit on the
        # notification object.
        queue_override = getattr(notification, "_queue", None)
        if queue_override:
            job.on_queue(queue_override)
        delay_seconds = getattr(notification, "_delay", None) or 0
        if delay_seconds:
            Queue.dispatch_after(job, delay_seconds)
        else:
            Queue.dispatch(job)
        return True

    def _send_now(self, notifiable, notification) -> bool:
        """
        Send a notification immediately (synchronously).

        Args:
            notifiable: The entity to notify
            notification: The notification to send

        Returns:
            True if sent successfully, False otherwise
        """
        # Get channels for this notification
        channels = self._get_channels(notifiable, notification)

        # Send to each channel — ``should_send`` is consulted PER CHANNEL
        # (Laravel parity: shouldSend(notifiable, channel)), so a
        # notification can, e.g., skip mail but still hit the database.
        results = []
        for channel_name in channels:
            # A miss is configuration damage, not an optional no-op. Silently
            # skipping it reports success when another channel lands and loses
            # the requested delivery forever.
            channel = self.channel(channel_name)
            if not self._should_send(notifiable, notification, channel_name):
                continue
            result = channel.send(notifiable, notification)
            results.append(result)

        # Return True if at least one channel succeeded
        return any(results) if results else False

    def send_now(self, notifiable, notification) -> bool:
        """
        Send a notification immediately (bypassing queue).

        Args:
            notifiable: The entity to notify
            notification: The notification to send

        Returns:
            True if sent successfully, False otherwise
        """
        return self._send_now(notifiable, notification)

    def send_delayed(self, notifiable, notification, delay_seconds: int) -> bool:
        """
        Send a notification with a delay.

        Args:
            notifiable: The entity to notify
            notification: The notification to send
            delay_seconds: Seconds to delay

        Returns:
            True if queued successfully, False otherwise
        """
        if delay_seconds <= 0:
            raise InvalidArgumentException("Notification delay must be positive.")
        delay = getattr(notification, "delay", None)
        if not callable(delay):
            raise InvalidArgumentException(
                "Delayed notifications must implement delay(seconds)."
            )
        delay(delay_seconds)
        # This method is an explicit queueing API: unlike send(), it must not
        # turn a non-ShouldQueue notification into an immediate delivery.
        return self._queue_notification(notifiable, notification)

    def _get_channels(self, notifiable, notification) -> list[str]:
        """
        Get the channels for a notification.

        Args:
            notifiable: The notifiable entity
            notification: The notification instance

        Returns:
            List of channel names
        """
        via = getattr(notification, "via", None)
        if not callable(via):
            raise InvalidArgumentException(
                "Notifications must implement via(notifiable)."
            )
        declared = via(notifiable)
        if isinstance(declared, str) or not isinstance(declared, (list, tuple)):
            raise InvalidArgumentException(
                "Notification via() must return a list or tuple of channel names."
            )
        channels: list[str] = []
        seen: set[str] = set()
        for channel in declared:
            if not isinstance(channel, str) or not channel.strip():
                raise InvalidArgumentException(
                    "Notification channel names must be non-empty strings."
                )
            name = channel.strip()
            if name not in seen:
                channels.append(name)
                seen.add(name)
        return channels

    def _should_send(self, notifiable, notification, channel_name: str) -> bool:
        """
        Determine if a notification should be sent on one channel.

        Args:
            notifiable: The notifiable entity
            notification: The notification instance
            channel_name: The channel about to be used

        Returns:
            True if should send, False otherwise
        """
        if hasattr(notification, "should_send") and callable(notification.should_send):
            return notification.should_send(notifiable, channel_name)
        return True

    def available_channels(self) -> list[str]:
        """
        Get list of available channels.

        Returns:
            List of available channel names
        """
        return list(self._channels.keys())
