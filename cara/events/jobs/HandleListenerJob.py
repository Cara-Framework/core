"""
Queue job for handling event listeners in background.

This job is automatically created when event listeners implement ShouldQueue.
"""

from cara.queues.contracts import BaseJob


class HandleListenerJob(BaseJob):
    """
    Job to handle event listeners in background.

    This is Laravel-style: when a listener implements ShouldQueue,
    the Event dispatcher automatically creates this job and dispatches it.
    """

    # Event-specific queue settings
    default_queue = "events"

    def __init__(self, listener, event):
        """
        Initialize the listener job.

        Args:
            listener: The listener to handle
            event: The event that triggered the listener
        """
        self.listener = listener
        self.event = event
        # BaseJob automatically handles initialization
        super().__init__(payload={"listener_type": type(listener).__name__})

    def handle(self):
        """
        Execute the job - handle the event with the listener.
        """
        # Execute the listener's handle method with the event
        self.listener.handle(self.event)
