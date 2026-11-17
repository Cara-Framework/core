"""SMTP transport — one message, one recipient, at most once.

The product-blind wire every product's transactional sender rides: build a
multipart message with CRLF-scrubbed headers, then drive ONE SMTP
transaction phase by phase with a bounded retry ladder. The caller keeps
everything that is product vocabulary — which sender address a stream
leaves from, templates, links, dev allowlists, system-recipient guards,
metrics — and hands this module a finished ``Message`` plus the envelope.

The delivery contract (:class:`~cara.mail.MailDeliveryOutcome`):

* Loss BEFORE the server's 354 to ``DATA`` is certain non-delivery and is
  retried with exponential backoff.
* A structured SMTP refusal follows its status family: 4xx retries, 5xx
  stops. A 5xx before any recipient was named judges our own account
  (credentials, sending domain), not this mail, so it raises
  :class:`SmtpAccountRefusedError` instead of returning a verdict a ledger
  could seal.
* Loss AFTER 354 is ambiguous — the body may already be with the provider —
  so the send is NEVER replayed and answers ``UNKNOWN_DELIVERY``. The one
  exception is a message the caller marks ``replay_safe`` (it carries the
  provider's idempotency key): that is sent once more, and from then on
  any answer but an accepted send reads ``UNKNOWN_DELIVERY``, since a
  refusal may be the provider refusing the duplicate of a delivered mail.

Logs are metadata only: the recipient is masked and neither the subject
nor any SMTP reply text (both can echo addresses) reaches log storage.
"""

from __future__ import annotations

import smtplib
import ssl
import time
from collections.abc import Mapping, Sequence
from email import encoders
from email.message import Message
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from cara.facades import Log
from cara.mail.MailDeliveryOutcome import MailDeliveryOutcome
from cara.mail.SmtpAccountRefusedError import SmtpAccountRefusedError
from cara.mail.SmtpSettings import SmtpSettings
from cara.mail.SmtpWire import (
    classify_refused_recipients,
    close_smtp,
    smtp_message_bytes,
    smtp_response_outcome,
    smtp_success,
)
from cara.support import email_mask, strip_header_crlf

MAIL_SEND_MAX_ATTEMPTS = 3
MAIL_RETRY_BASE_DELAY_SECONDS = 0.5

#: Network loss: retryable before ``DATA``, ambiguous after it.
_TRANSIENT = (
    smtplib.SMTPServerDisconnected,
    TimeoutError,
    ConnectionError,
    OSError,
)


def masked_recipient(address: str) -> str:
    """``cara.support.email_mask`` for log lines, never empty (``***``)."""
    try:
        return email_mask(address or "") or "***"
    except Exception:
        return "***"


def build_alternative_message(
    *,
    subject: str,
    from_name: str,
    from_email: str,
    to_email: str,
    text_body: str,
    html_body: str,
    reply_to: str = "",
    headers: Mapping[str, str] | None = None,
    attachments: Sequence[tuple[str, str, str]] | None = None,
) -> Message:
    """A text + html message with every header scrubbed of CR / LF.

    ``MIMEMultipart``'s compat32 policy assigns header values literally, so
    a stray CRLF in user-influenced content (the recipient, a subject, a
    header assembled from a URL) would inject a second header. Parts are
    always utf-8 (base64): an all-ASCII part would leave as 7bit, unwrapped,
    and a one-line HTML body runs past RFC 5321's 998-octet line cap.

    ``headers`` are applied in order, each REPLACING a header of the same
    name already present (``Reply-To`` included), never duplicating it;
    an empty name or value is skipped. ``attachments`` are ``(filename,
    content, mime_type)`` text triples riding beside the alternative part.
    """
    body = MIMEMultipart("alternative")
    body.attach(MIMEText(text_body, "plain", "utf-8"))
    body.attach(MIMEText(html_body, "html", "utf-8"))
    if attachments:
        message = MIMEMultipart("mixed")
        message.attach(body)
        for filename, content, mime_type in attachments:
            maintype, _, subtype = (mime_type or "text/plain").partition("/")
            part = MIMEBase(maintype, subtype or "plain")
            part.set_payload(content.encode("utf-8"))
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                "attachment",
                filename=strip_header_crlf(filename),
            )
            message.attach(part)
    else:
        message = body
    message["Subject"] = strip_header_crlf(subject)
    message["From"] = f"{strip_header_crlf(from_name)} <{strip_header_crlf(from_email)}>"
    message["To"] = strip_header_crlf(to_email)
    safe_reply_to = strip_header_crlf(reply_to).strip()
    if safe_reply_to:
        message["Reply-To"] = safe_reply_to
    for header_name, header_value in (headers or {}).items():
        safe_name = strip_header_crlf(str(header_name)).strip()
        safe_value = strip_header_crlf(str(header_value)).strip()
        if not safe_name or not safe_value:
            continue
        del message[safe_name]
        message[safe_name] = safe_value
    return message


def _backoff(attempt: int) -> bool:
    """Sleep this attempt's delay; False (no sleep) on the last attempt."""
    if attempt >= MAIL_SEND_MAX_ATTEMPTS - 1:
        return False
    time.sleep(MAIL_RETRY_BASE_DELAY_SECONDS * (2**attempt))
    return True


def _unknown_delivery(
    exc: BaseException, recipient: str, category: str
) -> MailDeliveryOutcome:
    Log.error(
        f"[EMAIL] Delivery outcome unknown for {masked_recipient(recipient)} "
        f"error_type={type(exc).__name__}",
        category=category,
        exc_info=False,
    )
    return MailDeliveryOutcome(MailDeliveryOutcome.UNKNOWN_DELIVERY, type(exc).__name__)


def _connect(settings: SmtpSettings) -> smtplib.SMTP:
    context = ssl.create_default_context()
    if settings.encryption == "ssl":
        server: smtplib.SMTP = smtplib.SMTP_SSL(
            settings.host, settings.port, timeout=settings.timeout, context=context
        )
    else:
        server = smtplib.SMTP(settings.host, settings.port, timeout=settings.timeout)
    try:
        server.ehlo()
        if settings.encryption == "tls":
            server.starttls(context=context)
            server.ehlo()
        if settings.username:
            server.login(settings.username, settings.password)
    except BaseException:
        close_smtp(server)
        raise
    return server


def send_smtp(
    message: Message,
    *,
    envelope_from: str,
    recipient: str,
    settings: SmtpSettings,
    replay_safe: bool = False,
    category: str = "mail",
) -> MailDeliveryOutcome:
    """Deliver ``message`` to ONE recipient under the at-most-once ladder.

    ``envelope_from`` / ``recipient`` are the SMTP envelope (scrubbed here
    too, so a hostile address cannot smuggle a command past the headers).
    ``replay_safe`` says the message carries the provider's idempotency key.
    Raises :class:`SmtpAccountRefusedError` for a 5xx before RCPT; every
    other failure is an outcome, never an exception.
    """
    safe_from = strip_header_crlf(envelope_from).strip()
    safe_to = strip_header_crlf(recipient).strip()
    # The exception of the attempt whose outcome was lost, once one was.
    ambiguous: Exception | None = None
    last_exc: Exception | None = None
    for attempt in range(MAIL_SEND_MAX_ATTEMPTS):
        recipient_named = False
        submission_started = False
        accepted = False
        try:
            server = _connect(settings)
            try:
                # Phase by phase, not ``sendmail()``: it hides whether a drop
                # happened during MAIL/RCPT (safe to retry) or after 354
                # (the body may be with the provider; replay can duplicate).
                mail_code, mail_reply = server.mail(safe_from)
                if not smtp_success(mail_code):
                    raise smtplib.SMTPSenderRefused(mail_code, mail_reply, safe_from)
                recipient_named = True
                rcpt_code, rcpt_reply = server.rcpt(safe_to)
                if not smtp_success(rcpt_code):
                    raise smtplib.SMTPRecipientsRefused(
                        {safe_to: (rcpt_code, rcpt_reply)}
                    )
                data_code, data_reply = server.docmd("DATA")
                if int(data_code) != 354:
                    raise smtplib.SMTPDataError(data_code, data_reply)
                submission_started = True
                server.send(smtp_message_bytes(message))
                final_code, final_reply = server.getreply()
                if int(final_code) != 250:
                    raise smtplib.SMTPDataError(final_code, final_reply)
                accepted = True
                Log.info(
                    f"mail sent to {masked_recipient(recipient)} "
                    f"outcome=sent smtp_code={int(final_code)}",
                    category=category,
                )
                return MailDeliveryOutcome(MailDeliveryOutcome.SENT)
            finally:
                close_smtp(server)
        except smtplib.SMTPRecipientsRefused as exc:
            if accepted:
                return MailDeliveryOutcome(MailDeliveryOutcome.SENT)
            if ambiguous is not None:
                return _unknown_delivery(ambiguous, recipient, category)
            outcome = classify_refused_recipients(exc.recipients, (safe_to,))
            if outcome.status == MailDeliveryOutcome.RETRYABLE_FAILURE and _backoff(
                attempt
            ):
                last_exc = exc
                continue
            Log.error(
                f"[EMAIL] Recipient refusal for {masked_recipient(recipient)} "
                f"outcome={outcome.status} reason={outcome.reason}",
                category=category,
                exc_info=False,
            )
            return outcome
        except (smtplib.SMTPResponseException, smtplib.SMTPNotSupportedError) as exc:
            if accepted:
                return MailDeliveryOutcome(MailDeliveryOutcome.SENT)
            if ambiguous is not None:
                return _unknown_delivery(ambiguous, recipient, category)
            # A server with no STARTTLS or AUTH has no code; it is a 502's verdict.
            outcome = smtp_response_outcome(
                getattr(exc, "smtp_code", 502), type(exc).__name__
            )
            if outcome.status == MailDeliveryOutcome.RETRYABLE_FAILURE and _backoff(
                attempt
            ):
                last_exc = exc
                continue
            Log.error(
                f"[EMAIL] SMTP response failure to {masked_recipient(recipient)} "
                f"outcome={outcome.status} reason={outcome.reason}",
                category=category,
                exc_info=False,
            )
            if (
                not recipient_named
                and outcome.status == MailDeliveryOutcome.PERMANENT_FAILURE
            ):
                raise SmtpAccountRefusedError(outcome) from exc
            return outcome
        except _TRANSIENT as exc:
            if accepted:
                return MailDeliveryOutcome(MailDeliveryOutcome.SENT)
            if submission_started or ambiguous is not None:
                if replay_safe and ambiguous is None and _backoff(attempt):
                    ambiguous = exc
                    continue
                return _unknown_delivery(ambiguous or exc, recipient, category)
            last_exc = exc
            if _backoff(attempt):
                continue
        except Exception as exc:
            if accepted:
                return MailDeliveryOutcome(MailDeliveryOutcome.SENT)
            if submission_started or ambiguous is not None:
                return _unknown_delivery(ambiguous or exc, recipient, category)
            last_exc = exc
            break
    if ambiguous is not None:
        return _unknown_delivery(ambiguous, recipient, category)
    failure_type = type(last_exc).__name__ if last_exc is not None else "smtp_failure"
    Log.error(
        f"[EMAIL] Failed to send to {masked_recipient(recipient)} after retries "
        f"error_type={failure_type}",
        category=category,
        exc_info=False,
    )
    return MailDeliveryOutcome(MailDeliveryOutcome.RETRYABLE_FAILURE, failure_type)


__all__ = [
    "MAIL_RETRY_BASE_DELAY_SECONDS",
    "MAIL_SEND_MAX_ATTEMPTS",
    "build_alternative_message",
    "masked_recipient",
    "send_smtp",
]
