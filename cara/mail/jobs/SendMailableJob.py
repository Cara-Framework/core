"""
Queue job for sending mailables in background.

This job is automatically created when mailables implement ShouldQueue.
"""

from cara.queues.contracts import BaseJob


class SendMailableJob(BaseJob):
    """
    Job to send mailables in background.

    This is Laravel-style: when a mailable implements ShouldQueue,
    the Mail manager automatically creates this job and dispatches it.
    """

    # Mail-specific queue settings
    default_queue = "emails"
    default_retry_attempts = 5  # Mail needs more retries

    def __init__(self, mailable, driver_name=None):
        """
        Initialize the mailable job.

        Args:
            mailable: The mailable to send
            driver_name: Optional driver name
        """
        self.mailable = mailable
        self.driver_name = driver_name
        # BaseJob automatically handles initialization
        super().__init__(payload={"mailable_type": type(mailable).__name__})

    def handle(self):
        """
        Execute the job - send the mailable.
        """
        # Get the mail service from container
        from cara.facades import Mail

        # Send the mailable immediately (bypass queue check)
        result = Mail._send_now(self.mailable, self.driver_name)

        if not result:
            raise Exception("Failed to send mailable through driver")
