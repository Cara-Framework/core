"""SMTP wire — what one transaction's replies mean, and the bytes it sends.

The pure half of the transport (``SmtpTransport.send_smtp`` drives the
conversation): classifying a refusal by its status family, closing a
connection without revising a known outcome, and rendering one
RFC-compliant, dot-stuffed DATA payload.
"""

from __future__ import annotations

import smtplib
from collections.abc import Mapping
from email import policy
from email.mime.multipart import MIMEMultipart

from cara.mail.MailDeliveryOutcome import MailDeliveryOutcome


def smtp_response_outcome(code: object, reason: str) -> MailDeliveryOutcome:
    """Classify an explicit SMTP response without crossing ambiguity rails."""
    try:
        numeric_code = int(code)
    except TypeError, ValueError:
        numeric_code = None
    status = (
        MailDeliveryOutcome.PERMANENT_FAILURE
        if numeric_code is not None and 500 <= numeric_code < 600
        else MailDeliveryOutcome.RETRYABLE_FAILURE
    )
    suffix = f":{numeric_code}" if numeric_code is not None else ""
    return MailDeliveryOutcome(status, f"{reason}{suffix}")


def _refusal_code(detail: object) -> object:
    if isinstance(detail, (tuple, list)) and detail:
        return detail[0]
    return getattr(detail, "smtp_code", detail)


def classify_refused_recipients(
    refused: Mapping[object, object],
    expected_recipients: tuple[str, ...],
) -> MailDeliveryOutcome:
    """Aggregate ``sendmail`` refusal details without unsafe batch replay.

    The public transport currently sends one recipient per call. Keeping this
    aggregation deterministic still prevents a future multi-recipient caller
    from replaying a message already accepted for only part of its audience.
    """
    if not refused:
        return MailDeliveryOutcome(MailDeliveryOutcome.SENT)

    expected = tuple(
        dict.fromkeys(
            str(address).strip().casefold()
            for address in expected_recipients
            if str(address).strip()
        )
    )
    refused_by_address = {
        str(address).strip().casefold(): detail for address, detail in refused.items()
    }
    matched = [
        refused_by_address[address]
        for address in expected
        if address in refused_by_address
    ]
    accepted_count = len(expected) - len(matched)
    if accepted_count:
        return MailDeliveryOutcome(
            MailDeliveryOutcome.UNKNOWN_DELIVERY,
            ("partial_recipient_delivery" if matched else "unexpected_recipient_refusal"),
        )

    outcomes = [
        smtp_response_outcome(_refusal_code(detail), "recipient_refused")
        for detail in matched
    ]
    if any(
        outcome.status == MailDeliveryOutcome.PERMANENT_FAILURE for outcome in outcomes
    ):
        reason = (
            "mixed_recipient_refusal"
            if any(
                outcome.status == MailDeliveryOutcome.RETRYABLE_FAILURE
                for outcome in outcomes
            )
            else "recipient_refused_5xx"
        )
        return MailDeliveryOutcome(
            MailDeliveryOutcome.PERMANENT_FAILURE,
            reason,
        )
    return MailDeliveryOutcome(
        MailDeliveryOutcome.RETRYABLE_FAILURE,
        "recipient_refused_4xx",
    )


def close_smtp(server: object) -> None:
    """Close without letting QUIT-path loss overwrite a known send outcome."""
    try:
        quit_method = getattr(server, "quit", None)
        if callable(quit_method):
            quit_method()
            return
    except OSError, smtplib.SMTPException:
        # QUIT failure cannot revise a send outcome already received. The
        # transport still attempts the lower-level close below.
        pass
    try:
        close_method = getattr(server, "close", None)
        if callable(close_method):
            close_method()
    except OSError, smtplib.SMTPException:
        # The message outcome is already known; socket teardown is advisory.
        pass


def smtp_success(code: object) -> bool:
    try:
        numeric_code = int(code)
    except TypeError, ValueError:
        return False
    return 200 <= numeric_code < 300


#: RFC 5322's hard limit. Only an all-ASCII header folds against it, so an
#: unbreakable token (a ~150-char unsubscribe URL) is never rewritten as an
#: RFC 2047 encoded word, which no provider decodes in ``List-Unsubscribe``.
_ASCII_HEADER_POLICY = policy.SMTP.clone(max_line_length=998)


class _WirePolicy(policy.EmailPolicy):
    """Non-ASCII keeps the 78-column fold: RFC 2047 caps an encoded word at 75."""

    def fold_binary(self, name: str, value: str) -> bytes:
        if str(value).isascii():
            return _ASCII_HEADER_POLICY.fold_binary(name, value)
        return super().fold_binary(name, value)


def smtp_message_bytes(message: MIMEMultipart) -> bytes:
    """Render one RFC-compliant, dot-stuffed SMTP DATA payload."""
    payload = message.as_bytes(policy=_WirePolicy(linesep="\r\n"))
    if payload.startswith(b"."):
        payload = b"." + payload
    payload = payload.replace(b"\r\n.", b"\r\n..")
    if not payload.endswith(b"\r\n"):
        payload += b"\r\n"
    return payload + b".\r\n"


__all__ = [
    "classify_refused_recipients",
    "close_smtp",
    "smtp_message_bytes",
    "smtp_response_outcome",
    "smtp_success",
]
