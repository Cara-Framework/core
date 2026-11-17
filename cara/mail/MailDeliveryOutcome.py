"""The transport result every mail send answers with (DOCTRINE §5).

Kept apart from :mod:`cara.mail.SmtpTransport` so callers that only branch
on the outcome — delivery outboxes, notice sweeps — depend on the
vocabulary and not on the SMTP transport.

``UNKNOWN_DELIVERY`` is the honest state after a timeout mid-``DATA``: the
message may or may not have reached the server, so callers must not count it
as sent and must not blindly re-send either (DOCTRINE §7: unknown is not a
fabricated success or failure).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MailDeliveryOutcome:
    """Transport result with an explicit SMTP uncertainty boundary."""

    status: str
    reason: str = ""

    SENT = "sent"
    NOT_CONFIGURED = "not_configured"
    RETRYABLE_FAILURE = "retryable_failure"
    PERMANENT_FAILURE = "permanent_failure"
    UNKNOWN_DELIVERY = "unknown_delivery"

    @property
    def sent(self) -> bool:
        return self.status == self.SENT
