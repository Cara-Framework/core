"""
Pending mail send operation for Cara Framework.

This class allows chaining operations before sending emails,
similar to Laravel's mail queueing system.
"""


class MailPendingSend:
    def __init__(self, manager, mailable):
        """
        Initialize pending send.
        """
        self.manager = manager
        self.mailable = mailable
        self.driver_name = None

    def driver(self, name: str):
        """
        Set the driver to use for sending.
        """
        self.driver_name = name
        return self

    def send(self) -> bool:
        """
        Send the mailable.
        """
        return self.manager.send(self.mailable, self.driver_name)
