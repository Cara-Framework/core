"""
Queue job for sending mailables in background.

This job is automatically created when mailables implement ShouldQueue.
"""

from __future__ import annotations

from cara.context import ExecutionContext
from cara.exceptions import CaraException
from cara.facades import Mail
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

    async def handle(self):
        """
        Execute the job - send the mailable.

        ``handle`` MUST be async: ``SignedJsonJobSerializer`` rejects a
        sync ``handle`` at both serialize and deserialize, and
        ``queue:work`` repeats the gate consumer-side, so a sync handler
        made this job undispatchable on the AMQP rail — the only rail
        cara ships. A queued Mailable therefore never sent, and
        ``Mail._queue_mailable`` swallowed the rejection, so nobody saw it.

        ``Mail._send_now`` is blocking SMTP socket I/O, so it crosses the
        boundary through ``ExecutionContext.run_in_thread`` (which copies
        the contextvar snapshot — job id, correlation id, tenancy — and
        gives the thread its own DB connection registry) rather than
        stalling the worker's event loop.
        """
        # Get the mail service from container

        # Send the mailable immediately (bypass queue check)
        result = await ExecutionContext.run_in_thread(
            Mail._send_now,
            self.mailable,
            self.driver_name,
        )

        if not result:
            raise CaraException("Failed to send mailable through driver")
