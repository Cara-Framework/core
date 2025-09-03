"""
Mail message builder for Cara Framework.

This class provides a quick way to send emails without
creating custom Mailable classes.
"""

from typing import Optional

from cara.mail import Mailable


class MailMessage:
    def __init__(self, manager):
        """
        Initialize mail message.
        """
        self.manager = manager
        self.mailable = Mailable()

    def to(self, addresses):
        """Set recipient addresses."""
        self.mailable.to(addresses)
        return self

    def from_(self, address):
        """Set sender address."""
        self.mailable.from_(address)
        return self

    def cc(self, addresses):
        """Set CC addresses."""
        self.mailable.cc(addresses)
        return self

    def bcc(self, addresses):
        """Set BCC addresses."""
        self.mailable.bcc(addresses)
        return self

    def subject(self, subject):
        """Set subject."""
        self.mailable.subject(subject)
        return self

    def view(self, template, data=None):
        """Set view template."""
        self.mailable.view(template, data)
        return self

    def text(self, content):
        """Set text content."""
        self.mailable.text(content)
        return self

    def html(self, content):
        """Set HTML content."""
        self.mailable.html(content)
        return self

    def attach(self, name, path):
        """Add attachment."""
        self.mailable.attach(name, path)
        return self

    def send(self, driver_name: Optional[str] = None) -> bool:
        """
        Send the message.
        """
        return self.manager.send(self.mailable, driver_name)

    def queue(self, driver_name: Optional[str] = None) -> bool:
        """
        Queue the message for background processing.
        """
        try:
            from cara.facades import Queue
            from cara.mail.jobs import SendMailableJob

            # Build the mailable first to avoid references issues
            self.mailable.set_application(self.manager.application)
            self.mailable.build()

            # Clear application reference to avoid circular reference
            self.mailable.set_application(None)

            # Create job with the mailable - SerializesModels will handle serialization
            job = SendMailableJob(self.mailable, driver_name)
            Queue.dispatch(job)
            return True

        except Exception as e:
            from cara.facades import Log

            Log.error(f"Failed to queue mail message: {e}")
            return False
