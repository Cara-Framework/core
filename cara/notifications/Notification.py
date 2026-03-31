"""
Central Notification Manager for the Cara framework.

This module provides the Notification class, which manages multiple notification channels and delegates
notification operations to the appropriate channel instances.
"""

from typing import List

from cara.exceptions import DriverNotRegisteredException
from cara.facades import Log, Queue
from cara.notifications.contracts import NotificationChannel
from cara.queues.contracts import ShouldQueue


class Notification:
    """
    Central notification manager. Delegates notification sending to registered channel instances.

    The default channels are injected via constructor (from NotificationProvider).
    """

    def __init__(self, application, default_channels: List[str]):
        """
        Initialize notification manager.

        Args:
            application: Cara application instance
            default_channels: List of default channel names
        """
        self.application = application
        self._channels: dict[str, NotificationChannel] = {}
        self._default_channels: List[str] = default_channels

    def add_channel(self, name: str, channel: NotificationChannel) -> None:
        """Register a channel instance under `name`."""
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
        try:
            # Create a job to send the notification
            from cara.notifications.jobs import SendNotificationJob

            job = SendNotificationJob(notifiable, notification)

            # Dispatch to queue using facade
            Queue.dispatch(job)
            return True

        except Exception as e:
            # Log error using facade
            Log.error(f"Failed to queue notification: {e}")
            return False

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

        # Check if notification should be sent
        if not self._should_send(notifiable, notification):
            return False

        # Send to each channel
        results = []
        for channel_name in channels:
            channel = self._channels.get(channel_name)
            if channel:
                result = self._send_via_channel(
                    channel, channel_name, notifiable, notification
                )
                results.append(result)

        # Return True if at least one channel succeeded
        return any(results) if results else False

    def _send_via_channel(
        self, channel, channel_name: str, notifiable, notification
    ) -> bool:
        """
        Send notification via specific channel with Laravel-style method resolution.

        Args:
            channel: Channel instance
            channel_name: Name of the channel
            notifiable: The notifiable entity
            notification: The notification instance

        Returns:
            True if sent successfully, False otherwise
        """
        # Check if notification has channel-specific method (Laravel style)
        method_name = f"to_{channel_name}"
        if hasattr(notification, method_name):
            channel_data = getattr(notification, method_name)(notifiable)
            if channel_data is not None:
                # Create a wrapper notification with the channel data
                wrapper_notification = type(
                    "NotificationWrapper",
                    (),
                    {"data": channel_data, "original": notification},
                )()
                return channel.send(notifiable, wrapper_notification)

        # Fallback to regular send
        return channel.send(notifiable, notification)

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
        # In a real implementation, this would be queued
        # For now, just set the delay on the notification and send
        if hasattr(notification, "delay"):
            notification.delay(delay_seconds)
        return self.send(notifiable, notification)

    def _get_channels(self, notifiable, notification) -> List[str]:
        """
        Get the channels for a notification.

        Args:
            notifiable: The notifiable entity
            notification: The notification instance

        Returns:
            List of channel names
        """
        if hasattr(notification, "via") and callable(notification.via):
            return notification.via(notifiable)
        return self._default_channels

    def _should_send(self, notifiable, notification) -> bool:
        """
        Determine if a notification should be sent.

        Args:
            notifiable: The notifiable entity
            notification: The notification instance

        Returns:
            True if should send, False otherwise
        """
        if hasattr(notification, "should_send") and callable(notification.should_send):
            return notification.should_send(notifiable, "all")
        return True

    def extend(self, name: str, channel: NotificationChannel) -> None:
        """
        Register a custom channel (alias for add_channel).

        Args:
            name: Channel name
            channel: Channel instance
        """
        self.add_channel(name, channel)

    def get_default_channels(self) -> List[str]:
        """
        Get default notification channels.

        Returns:
            List of default channel names
        """
        return self._default_channels.copy()

    def set_default_channels(self, channels: List[str]) -> None:
        """
        Set default notification channels.

        Args:
            channels: List of channel names
        """
        self._default_channels = channels

    def available_channels(self) -> List[str]:
        """
        Get list of available channels.

        Returns:
            List of available channel names
        """
        return list(self._channels.keys())
