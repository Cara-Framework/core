"""``SmtpAccountRefusedError``: the SMTP server refused our own account."""

from __future__ import annotations

from cara.exceptions import MailSendException
from cara.mail.MailDeliveryOutcome import MailDeliveryOutcome


class SmtpAccountRefusedError(MailSendException, RuntimeError):
    """The server refused OUR account (5xx before any recipient was named).

    Nothing can have been delivered, and no recipient will fare better: a
    rotated password or an unverified sending domain. The caller decides
    how loud that is; it is never a per-mail verdict.
    """

    def __init__(self, outcome: MailDeliveryOutcome) -> None:
        super().__init__(f"SMTP refused our own account ({outcome.reason})")
        self.outcome = outcome


__all__ = ["SmtpAccountRefusedError"]
