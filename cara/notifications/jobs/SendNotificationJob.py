"""
Queue job for sending notifications in background.

This job is automatically created when notifications implement ShouldQueue.
Uses BaseJob which includes SerializesModels for proper serialization.
"""

from __future__ import annotations

from cara.context import ExecutionContext
from cara.exceptions import CaraException
from cara.facades import Notification
from cara.queues.contracts import BaseJob


class SendNotificationJob(BaseJob):
    """
    Job to send notifications in background.

    This is Laravel-style: when a notification implements ShouldQueue,
    the Notification manager automatically creates this job and dispatches it.

    BaseJob already includes SerializesModels for proper serialization.
    """

    def __init__(self, notifiable, notification):
        """
        Initialize the notification job.

        Args:
            notifiable: The entity to notify
            notification: The notification to send
        """
        # Store objects directly - BaseJob/SerializesModels will handle serialization
        self.notifiable = notifiable
        self.notification = notification
        # BaseJob automatically handles initialization
        super().__init__(payload={"notification_type": type(notification).__name__})

    async def handle(self):
        """
        Execute the job - send the notification.

        ``handle`` MUST be async: ``SignedJsonJobSerializer`` rejects a
        sync ``handle`` at both serialize and deserialize, and
        ``queue:work`` repeats the gate consumer-side, so a sync handler
        made this job undispatchable on the AMQP rail — the only rail
        cara ships. ``Notification._queue_notification`` does not catch,
        so a queued notification raised straight into the caller.

        ``Notification._send_now`` fans out over blocking channel drivers
        (SMTP, HTTP), so it crosses the boundary through
        ``ExecutionContext.run_in_thread`` — which copies the contextvar
        snapshot and gives the thread its own DB connection registry —
        rather than stalling the worker's event loop.
        """
        # Objects are automatically reconstructed by SerializesModels
        # Get the notification service from container

        # Send the notification immediately (bypass queue check)
        result = await ExecutionContext.run_in_thread(
            Notification._send_now,
            self.notifiable,
            self.notification,
        )

        if not result:
            raise CaraException("Failed to send notification through channels")
