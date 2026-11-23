"""
Central Notification Manager for the Cara framework.

This module provides the Notification class, which manages multiple notification channels and delegates
notification operations to the appropriate channel instances.
"""

from __future__ import annotations

from cara.exceptions import DriverNotRegisteredException, InvalidArgumentException
from cara.facades import DB, Queue
from cara.notifications.contracts import NotificationChannel
from cara.notifications.jobs import SendNotificationJob
from cara.notifications.NothingDue import NothingDue
from cara.queues.contracts import ShouldQueue


class Notification:
    """
    Central notification manager. Delegates notification sending to registered channel instances.

    Each notification declares its channels through ``via(notifiable)``.
    """

    #: A send where ``should_send`` skipped every channel (see NothingDue).
    NOTHING_DUE = NothingDue()

    def __init__(self):
        """Initialize an empty channel registry."""
        self._channels: dict[str, NotificationChannel] = {}

    def add_channel(self, name: str, channel: NotificationChannel) -> None:
        """Register a channel instance under `name`."""
        if not isinstance(name, str) or not name.strip():
            raise InvalidArgumentException(
                "Notification channel name must be a non-empty string."
            )
        name = name.strip()
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
        if channel is None:
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

        # Reject malformed/unknown delivery topology before acknowledging the
        # enqueue. Otherwise the caller receives True for a job that can only
        # fail after it leaves the request boundary.
        channel_names = self._get_channels(notifiable, notification)
        for channel_name in channel_names:
            self.channel(channel_name)

        # Plain data, computed once: a notifiable or notification that cannot
        # be expressed as plain data is refused here, naming the field.
        notifiable_class, notifiable_data = SendNotificationJob.payload_of(
            notifiable, "notifiable"
        )
        notification_class, notification_data = SendNotificationJob.payload_of(
            notification, "notification"
        )
        # One job per channel: a retry replays its job's signed envelope
        # unchanged, so only a job that owns a single channel can retry
        # without re-delivering a sibling channel that already succeeded.
        jobs = [
            SendNotificationJob(
                notifiable_class,
                notifiable_data,
                notification_class,
                notification_data,
                [channel_name],
            )
            for channel_name in channel_names
        ]

        # Honor the notification's chainable queue/delay overrides
        # (``on_queue()`` / ``delay()``) — Laravel parity: they must
        # survive into the dispatched job, not just sit on the
        # notification object.
        queue_override = getattr(notification, "_queue", None)
        if queue_override is not None:
            if not isinstance(queue_override, str) or not queue_override.strip():
                raise InvalidArgumentException(
                    "Notification queue must be a non-empty string."
                )
            for job in jobs:
                job.on_queue(queue_override.strip())
        delay_seconds = getattr(notification, "_delay", None)
        if delay_seconds is not None:
            self._require_positive_delay(delay_seconds)
        self._dispatch_jobs(jobs, delay_seconds)
        return True

    @staticmethod
    def _dispatch_jobs(
        jobs: list[SendNotificationJob], delay_seconds: int | None
    ) -> None:
        """Queue every channel's job, all or none where the driver allows it.

        A transactional-outbox driver writes its delivery rows on the
        caller's connection, so one transaction makes the fan-out atomic: a
        failure on the third channel no longer leaves the first two queued
        behind an exception that invites the caller to notify again.
        """
        driver_of = getattr(Queue, "driver", None)
        driver = driver_of() if callable(driver_of) and len(jobs) > 1 else None
        if getattr(driver, "durable_transactional_outbox", False):
            with DB.transaction():
                Notification._push_jobs(jobs, delay_seconds)
            return
        Notification._push_jobs(jobs, delay_seconds)

    @staticmethod
    def _push_jobs(jobs: list[SendNotificationJob], delay_seconds: int | None) -> None:
        for job in jobs:
            if delay_seconds is not None:
                Queue.dispatch_after(job, delay_seconds)
            else:
                Queue.dispatch(job)

    def _send_now(
        self, notifiable, notification, *, only_channels: list[str] | None = None
    ) -> bool | NothingDue:
        """
        Send a notification immediately (synchronously).

        Args:
            notifiable: The entity to notify
            notification: The notification to send
            only_channels: Deliver on exactly these channels (a queued job's
                own channel) instead of asking ``via()`` again.

        Returns:
            True if every due channel delivered, False if one answered False,
            :data:`NOTHING_DUE` (falsy) when ``should_send`` skipped them all.
        """
        # Get channels for this notification. Resolve EVERY channel before
        # delivering on any: a miss is configuration damage, not an optional
        # no-op, and finding it after earlier channels delivered would make
        # each retry re-deliver them.
        names = (
            list(only_channels)
            if only_channels is not None
            else self._get_channels(notifiable, notification)
        )
        channels = [(channel_name, self.channel(channel_name)) for channel_name in names]

        # Send to each channel — ``should_send`` is consulted PER CHANNEL
        # (Laravel parity: shouldSend(notifiable, channel)), so a
        # notification can, e.g., skip mail but still hit the database.
        # One channel failing must not cost its siblings their delivery: each
        # is attempted, and the failures are raised once all have run.
        results: list[bool] = []
        failures: list[tuple[str, Exception]] = []
        for channel_name, channel in channels:
            try:
                if not self._should_send(notifiable, notification, channel_name):
                    continue
                result = channel.send(notifiable, notification)
                if not isinstance(result, bool):
                    raise InvalidArgumentException(
                        f"Notification channel {channel_name!r} send() must return a boolean."
                    )
            except Exception as exc:
                failures.append((channel_name, exc))
                continue
            results.append(result)

        if len(failures) == 1:
            # Exactly what the caller saw before: the channel's own exception.
            raise failures[0][1]
        if failures:
            raise ExceptionGroup(
                "notification channels failed: "
                + ", ".join(name for name, _exc in failures),
                [exc for _name, exc in failures],
            )

        # Every requested, eligible delivery must succeed. Treating one success
        # as success for the whole fan-out loses the failed delivery forever.
        # No eligible delivery at all is NOT a failure; it stays falsy so a
        # bool-reading caller is unchanged, but it is its own value.
        return all(results) if results else self.NOTHING_DUE

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
        self._require_positive_delay(delay_seconds)
        delay = getattr(notification, "delay", None)
        if not callable(delay):
            raise InvalidArgumentException(
                "Delayed notifications must implement delay(seconds)."
            )
        delay(delay_seconds)
        # This method is an explicit queueing API: unlike send(), it must not
        # turn a non-ShouldQueue notification into an immediate delivery.
        return self._queue_notification(notifiable, notification)

    @staticmethod
    def _require_positive_delay(delay_seconds: int) -> None:
        if (
            isinstance(delay_seconds, bool)
            or not isinstance(delay_seconds, int)
            or delay_seconds <= 0
        ):
            raise InvalidArgumentException(
                "Notification delay must be a positive integer number of seconds."
            )

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
            if name in seen:
                raise InvalidArgumentException(
                    f"Notification channel {name!r} is declared more than once."
                )
            channels.append(name)
            seen.add(name)
        if not channels:
            raise InvalidArgumentException(
                "Notification via() must declare at least one channel."
            )
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
            decision = notification.should_send(notifiable, channel_name)
            if not isinstance(decision, bool):
                raise InvalidArgumentException(
                    "Notification should_send() must return a boolean."
                )
            return decision
        return True
