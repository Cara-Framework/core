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
* A structured SMTP refusal follows its status family at EVERY phase —
  EHLO included: 4xx retries, 5xx stops. A 5xx before any recipient was named judges our own account
  (credentials, sending domain), not this mail, so it raises
  :class:`SmtpAccountRefusedError` instead of returning a verdict a ledger
  could seal.
* Loss AFTER 354 is ambiguous — the body may already be with the provider —
  so the send is NEVER replayed and answers ``UNKNOWN_DELIVERY``. The one
  exception is a message the caller marks ``replay_safe`` (it carries the
  provider's idempotency key): that is sent once more, and from then on
  any answer but an accepted send reads ``UNKNOWN_DELIVERY``, since a
  refusal may be the provider refusing the duplicate of a delivered mail.

A message that cannot be rendered, or an envelope naming a blank or
non-ASCII recipient or a non-ASCII sender, is refused before any
connection: certain non-delivery, ``PERMANENT_FAILURE``, never retried.

Logs are metadata only: the recipient is masked and neither the subject
nor any SMTP reply text (both can echo addresses) reaches log storage.
"""

from __future__ import annotations

import functools
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


def _backoff(attempt: int, max_attempts: int, base_delay: float) -> bool:
    """Sleep this attempt's delay; False (no sleep) on the last attempt."""
    if attempt >= max_attempts - 1:
        return False
    if base_delay > 0:
        time.sleep(base_delay * (2**attempt))
    return True


def _unknown_delivery(
    exc: BaseException, recipient: str, category: str
) -> MailDeliveryOutcome:
    Log.error(
        f"[EMAIL] Delivery outcome unknown for {recipient} "
        f"error_type={type(exc).__name__}",
        category=category,
        exc_info=False,
    )
    return MailDeliveryOutcome(MailDeliveryOutcome.UNKNOWN_DELIVERY, type(exc).__name__)


@functools.cache
def _verified_tls_context() -> ssl.SSLContext:
    """The one certificate-verifying TLS context every connection shares.

    ``create_default_context()`` parses the system CA bundle (~5 ms) each
    time; per connection that is pure waste, since ``SmtpSettings`` carries
    no per-connection TLS override (no custom CA, no verify-off). An
    ``SSLContext`` is safe to share across threads for wrapping sockets, and
    nothing here mutates it after creation. A future override must build its
    OWN context rather than touch this one, or it would weaken every send.
    """
    return ssl.create_default_context()


def _greet(server: smtplib.SMTP) -> None:
    """EHLO, judged by its status family like every other handshake reply.

    ``smtplib.ehlo()`` returns a refusal instead of raising, and the next
    step (STARTTLS, AUTH) then fails as "extension not supported" — a 502
    that read as our account being refused. A 4xx is the server saying "not
    now" (busy, greylisting) and must stay retryable. A 5xx may be a
    pre-ESMTP server, so fall back to HELO as ``smtplib`` itself does;
    only a refused HELO is a verdict.
    """
    code, reply = server.ehlo()
    if smtp_success(code):
        return
    try:
        permanent = 500 <= int(code) < 600
    except TypeError, ValueError:
        permanent = False
    if not permanent:
        raise smtplib.SMTPHeloError(code, reply)
    helo_code, helo_reply = server.helo()
    if not smtp_success(helo_code):
        raise smtplib.SMTPHeloError(helo_code, helo_reply)


def open_smtp_connection(settings: SmtpSettings) -> smtplib.SMTP:
    """Connect, greet, upgrade to TLS and log in — or close and raise.

    Every failure past the TCP connect closes the socket before raising, so
    a refused STARTTLS or login never leaks a connection. ``encryption`` is
    compared case-insensitively: ``"TLS"`` silently meaning plaintext (and
    AUTH in the clear) is the failure this guards.
    """
    mode = smtp_encryption_mode(settings.encryption)
    if mode == "ssl":
        server: smtplib.SMTP = smtplib.SMTP_SSL(
            settings.host,
            settings.port,
            timeout=settings.timeout,
            context=_verified_tls_context(),
        )
    else:
        server = smtplib.SMTP(settings.host, settings.port, timeout=settings.timeout)
    try:
        _greet(server)
        if mode == "tls":
            server.starttls(context=_verified_tls_context())
            _greet(server)
        if settings.username:
            server.login(settings.username, settings.password)
    except BaseException:
        close_smtp(server)
        raise
    return server


#: The encryption modes the transport speaks. ``none`` must be spelled out.
SMTP_ENCRYPTION_MODES = frozenset({"tls", "ssl", "none"})


def smtp_encryption_mode(value: object) -> str:
    """``value`` as a known encryption mode, or raise ``ValueError``.

    Anything but ``ssl`` / ``tls`` used to open a PLAINTEXT connection, so a
    typo (``"tsl"``, ``"starttls"``) or an empty setting silently sent
    credentials and mail in the clear. Plaintext is now only ever chosen by
    writing ``none``.
    """
    mode = str(value if value is not None else "").strip().lower()
    if mode not in SMTP_ENCRYPTION_MODES:
        raise ValueError(
            f"Unknown SMTP encryption {value!r}: use one of "
            f"{', '.join(sorted(SMTP_ENCRYPTION_MODES))}."
        )
    return mode


def _sender_refusal(sender: str) -> str:
    """Why this envelope sender can never be used, or "" when it can.

    A non-ASCII sender raises ``UnicodeEncodeError`` inside ``smtplib`` at
    ``MAIL FROM`` on every attempt, exactly like a non-ASCII recipient, and
    came back retryable. Without SMTPUTF8 it is permanent. (A blank sender is
    the legitimate null reverse-path, so it is left alone.)
    """
    return "sender_not_ascii" if not sender.isascii() else ""


def _recipient_refusal(recipients: tuple[str, ...]) -> str:
    """Why this envelope can never be delivered, or "" when it can.

    Blank and non-ASCII addresses are permanent per-message failures: a
    blank RCPT is refused (often as a 501 that classified as retryable) and a
    non-ASCII one raises ``UnicodeEncodeError`` inside ``smtplib`` — both
    used to come back retryable, so a durable ledger re-tried them forever.
    Non-ASCII would need SMTPUTF8, which this transport does not speak (no
    ``MAIL FROM ... SMTPUTF8``, no UTF-8 header policy), so it refuses.
    """
    if not recipients or any(not address for address in recipients):
        return "recipient_blank"
    if any(not address.isascii() for address in recipients):
        return "recipient_not_ascii"
    return ""


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
    return send_smtp_envelope(
        message,
        envelope_from=envelope_from,
        recipients=(recipient,),
        settings=settings,
        replay_safe=replay_safe,
        category=category,
    )


def send_smtp_envelope(
    message: Message,
    *,
    envelope_from: str,
    recipients: Sequence[str],
    settings: SmtpSettings,
    replay_safe: bool = False,
    category: str = "mail",
    max_attempts: int = MAIL_SEND_MAX_ATTEMPTS,
    retry_base_delay: float = MAIL_RETRY_BASE_DELAY_SECONDS,
) -> MailDeliveryOutcome:
    """:func:`send_smtp` for one transaction naming several recipients.

    All or nothing: every recipient is named before ``DATA``, and any
    refusal aborts the transaction before a byte of body leaves, so a
    partial audience never holds a message the caller is told failed (and
    a retry never duplicates it to the recipients that had accepted). Any
    5xx refusal is permanent; only an all-4xx refusal retries. BCC is
    simply a recipient here that no header names.
    """
    safe_from = strip_header_crlf(envelope_from).strip()
    safe_to = tuple(
        dict.fromkeys(strip_header_crlf(address).strip() for address in recipients)
    )
    label = masked_recipient(safe_to[0] if safe_to else "")
    if len(safe_to) > 1:
        label = f"{label} (+{len(safe_to) - 1})"
    refusal = _sender_refusal(safe_from) or _recipient_refusal(safe_to)
    if refusal:
        Log.error(
            f"[EMAIL] Undeliverable recipient for {label} "
            f"outcome={MailDeliveryOutcome.PERMANENT_FAILURE} reason={refusal}",
            category=category,
            exc_info=False,
        )
        return MailDeliveryOutcome(MailDeliveryOutcome.PERMANENT_FAILURE, refusal)
    # Render BEFORE any connection: a message the generator refuses (a raw
    # header with a bare newline, say) used to fail after the server's 354,
    # turning a certain non-delivery into UNKNOWN_DELIVERY. It is the same
    # bytes on every attempt, so the failure is permanent, not retryable.
    try:
        payload = smtp_message_bytes(message)
    except Exception as exc:
        Log.error(
            f"[EMAIL] Message for {label} could not be rendered, not sent "
            f"error_type={type(exc).__name__}",
            category=category,
            exc_info=False,
        )
        return MailDeliveryOutcome(
            MailDeliveryOutcome.PERMANENT_FAILURE,
            f"message_render_failed:{type(exc).__name__}",
        )
    attempts = max(int(max_attempts), 1)
    # The exception of the attempt whose outcome was lost, once one was.
    ambiguous: Exception | None = None
    last_exc: Exception | None = None
    for attempt in range(attempts):
        recipient_named = False
        submission_started = False
        accepted = False
        try:
            server = open_smtp_connection(settings)
            try:
                # Phase by phase, not ``sendmail()``: it hides whether a drop
                # happened during MAIL/RCPT (safe to retry) or after 354
                # (the body may be with the provider; replay can duplicate).
                mail_code, mail_reply = server.mail(safe_from)
                if not smtp_success(mail_code):
                    raise smtplib.SMTPSenderRefused(mail_code, mail_reply, safe_from)
                recipient_named = True
                refused: dict[str, tuple[int, bytes]] = {}
                for address in safe_to:
                    rcpt_code, rcpt_reply = server.rcpt(address)
                    if not smtp_success(rcpt_code):
                        refused[address] = (rcpt_code, rcpt_reply)
                if refused:
                    raise smtplib.SMTPRecipientsRefused(refused)
                data_code, data_reply = server.docmd("DATA")
                if int(data_code) != 354:
                    raise smtplib.SMTPDataError(data_code, data_reply)
                submission_started = True
                server.send(payload)
                final_code, final_reply = server.getreply()
                if int(final_code) != 250:
                    raise smtplib.SMTPDataError(final_code, final_reply)
                accepted = True
                Log.info(
                    f"mail sent to {label} outcome=sent smtp_code={int(final_code)}",
                    category=category,
                )
                return MailDeliveryOutcome(MailDeliveryOutcome.SENT)
            finally:
                close_smtp(server)
        except smtplib.SMTPRecipientsRefused as exc:
            if accepted:
                return MailDeliveryOutcome(MailDeliveryOutcome.SENT)
            if ambiguous is not None:
                return _unknown_delivery(ambiguous, label, category)
            # Judged on the refused addresses alone: the transaction was
            # aborted before DATA, so nobody — accepted or not — got a copy.
            outcome = classify_refused_recipients(
                exc.recipients, tuple(str(address) for address in exc.recipients)
            )
            if outcome.status == MailDeliveryOutcome.RETRYABLE_FAILURE and _backoff(
                attempt, attempts, retry_base_delay
            ):
                last_exc = exc
                continue
            Log.error(
                f"[EMAIL] Recipient refusal for {label} "
                f"outcome={outcome.status} reason={outcome.reason}",
                category=category,
                exc_info=False,
            )
            return outcome
        except (smtplib.SMTPResponseException, smtplib.SMTPNotSupportedError) as exc:
            if accepted:
                return MailDeliveryOutcome(MailDeliveryOutcome.SENT)
            if ambiguous is not None:
                return _unknown_delivery(ambiguous, label, category)
            # A server with no STARTTLS or AUTH has no code; it is a 502's verdict.
            outcome = smtp_response_outcome(
                getattr(exc, "smtp_code", 502), type(exc).__name__
            )
            if outcome.status == MailDeliveryOutcome.RETRYABLE_FAILURE and _backoff(
                attempt, attempts, retry_base_delay
            ):
                last_exc = exc
                continue
            Log.error(
                f"[EMAIL] SMTP response failure to {label} "
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
                if (
                    replay_safe
                    and ambiguous is None
                    and _backoff(attempt, attempts, retry_base_delay)
                ):
                    ambiguous = exc
                    continue
                return _unknown_delivery(ambiguous or exc, label, category)
            last_exc = exc
            if _backoff(attempt, attempts, retry_base_delay):
                continue
        except Exception as exc:
            if accepted:
                return MailDeliveryOutcome(MailDeliveryOutcome.SENT)
            if submission_started or ambiguous is not None:
                return _unknown_delivery(ambiguous or exc, label, category)
            last_exc = exc
            break
    if ambiguous is not None:
        return _unknown_delivery(ambiguous, label, category)
    failure_type = type(last_exc).__name__ if last_exc is not None else "smtp_failure"
    Log.error(
        f"[EMAIL] Failed to send to {label} after retries error_type={failure_type}",
        category=category,
        exc_info=False,
    )
    return MailDeliveryOutcome(MailDeliveryOutcome.RETRYABLE_FAILURE, failure_type)


__all__ = [
    "MAIL_RETRY_BASE_DELAY_SECONDS",
    "MAIL_SEND_MAX_ATTEMPTS",
    "SMTP_ENCRYPTION_MODES",
    "build_alternative_message",
    "masked_recipient",
    "open_smtp_connection",
    "send_smtp",
    "send_smtp_envelope",
    "smtp_encryption_mode",
]
