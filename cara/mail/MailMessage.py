"""
Mail message builder for Cara Framework.

This class provides a quick way to send emails without
creating custom Mailable classes.
"""

from __future__ import annotations

from email.utils import formataddr

from cara.mail.Mailable import Mailable


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

    def from_(self, address, name=None):
        """Set sender address, with an optional display name.

        ``MailChannel`` (and Laravel-style callers) pass a name alongside
        the address — ``message.from_(addr, "Cheapa")``. The previous
        one-arg signature raised ``TypeError`` on every such call, which
        ``MailChannel.send`` swallowed into a silent ``return False`` so the
        notification never sent. When a name is given, encode an RFC 5322
        ``"Name <addr>"`` value; ``SmtpDriver`` transmits via
        ``server.send_message``, which derives the bare envelope sender from
        this header, so the combined form is safe.
        """
        self.mailable.from_(formataddr((name, address)) if name else address)
        return self

    def reply_to(self, address):
        """Set the reply-to address.

        ``Mailable`` has always supported ``reply_to`` but ``MailMessage``
        never exposed it, so ``MailChannel``'s ``message.reply_to(...)`` call
        raised ``AttributeError`` (swallowed → silent send failure). Mirror
        the other fluent proxies.
        """
        self.mailable.reply_to(address)
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

    def send(self, driver_name: str | None = None) -> bool:
        """
        Send the message.
        """
        return self.manager.send(self.mailable, driver_name)

    def queue(self, driver_name: str | None = None) -> bool:
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

            Log.error("Failed to queue mail message: %s", e)
            return False
