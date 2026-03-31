"""
Queue job for sending notifications in background.

This job is automatically created when notifications implement ShouldQueue.
Uses BaseJob which includes SerializesModels for proper serialization.
"""

from cara.queues.contracts import BaseJob


class SendNotificationJob(BaseJob):
    """
    Job to send notifications in background.

    This is Laravel-style: when a notification implements ShouldQueue,
    the Notification manager automatically creates this job and dispatches it.

    BaseJob already includes SerializesModels for proper serialization.
    """

    # Notification-specific queue settings
    default_queue = "notifications"

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

    def handle(self):
        """
        Execute the job - send the notification.
        """
        # Objects are automatically reconstructed by SerializesModels
        # Get the notification service from container
        from cara.facades import Notification

        # Send the notification immediately (bypass queue check)
        result = Notification._send_now(self.notifiable, self.notification)

        if not result:
            raise Exception("Failed to send notification through channels")
