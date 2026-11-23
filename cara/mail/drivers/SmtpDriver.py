"""
SMTP Mail Driver for Cara Framework.

The ``Mail`` manager's ``smtp`` driver (``Mail.to(...).send()``, the
notification ``MailChannel``). It builds the message here and hands the
envelope to :func:`cara.mail.SmtpTransport.send_smtp_envelope`, the same
at-most-once ladder the products' transactional senders ride:

* loss before the server's 354 is retried with backoff; a refusal follows
  its status family (4xx retries, 5xx stops) at every phase;
* once the body may be with the server it is NEVER sent again — the old
  ``server.send_message`` loop re-sent an already accepted message on every
  attempt when the final reply or the QUIT was lost;
* every To, Cc AND Bcc address is named in the envelope (Bcc in no header),
  all or nothing: any refused recipient aborts before DATA.

``send`` answers a bool, so ``UNKNOWN_DELIVERY`` (maybe delivered) reads
``False`` — never a claimed success — and is logged as unknown.
"""

from __future__ import annotations

import contextlib
import mimetypes
import os
import smtplib
from email import encoders
from email.message import Message
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, getaddresses, make_msgid, parseaddr
from typing import Any

from cara.facades import Log
from cara.mail.contracts import MailContract
from cara.mail.Mailable import validate_custom_header
from cara.mail.MailDeliveryOutcome import MailDeliveryOutcome
from cara.mail.SmtpAccountRefusedError import SmtpAccountRefusedError
from cara.mail.SmtpSettings import SmtpSettings
from cara.mail.SmtpTransport import (
    open_smtp_connection,
    send_smtp_envelope,
    smtp_encryption_mode,
)
from cara.mail.SmtpWire import close_smtp
from cara.support import strip_header_crlf

#: Failures where the message is fine and the peer is momentarily
#: unavailable — worth another attempt. Kept as the published vocabulary;
#: the transport classifies by SMTP status family (4xx vs 5xx) itself.
TRANSIENT_SMTP_ERRORS: tuple[type[Exception], ...] = (
    smtplib.SMTPServerDisconnected,
    smtplib.SMTPConnectError,
    smtplib.SMTPHeloError,
    TimeoutError,
    ConnectionError,
    OSError,
)

#: Exception types whose 5xx form repeating cannot fix (their 4xx form is
#: retried by the transport — greylisting is a 451 RCPT refusal).
PERMANENT_SMTP_ERRORS: tuple[type[Exception], ...] = (
    smtplib.SMTPRecipientsRefused,
    smtplib.SMTPSenderRefused,
    smtplib.SMTPDataError,
    smtplib.SMTPAuthenticationError,
)


class SmtpDriver(MailContract):
    driver_name = "smtp"

    #: Total attempts for a transient failure, and the base of the
    #: exponential backoff between them. Overridable per app through the
    #: driver's configuration.
    DEFAULT_MAX_ATTEMPTS = 3
    DEFAULT_RETRY_BASE_DELAY = 1.0

    def __init__(self, config: dict[str, Any]):
        """
        Initialize SMTP driver with configuration.

        Args:
            config: SMTP configuration dictionary
        """
        self.config = config
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 587)
        self.username = config.get("username")
        self.password = config.get("password")
        # Validated at construction, the earliest point a typo is knowable:
        # an unknown value ("tsl", "starttls", "") used to fall through to a
        # PLAINTEXT connection with AUTH in the clear. Plaintext must be
        # spelled ``none``.
        self.encryption = smtp_encryption_mode(config.get("encryption", "tls"))
        self.timeout = config.get("timeout", 30)
        self.max_attempts = max(
            int(config.get("max_attempts", self.DEFAULT_MAX_ATTEMPTS)), 1
        )
        self.retry_base_delay = max(
            float(config.get("retry_base_delay", self.DEFAULT_RETRY_BASE_DELAY)), 0.0
        )

    def send(self, mailable_data: dict[str, Any]) -> bool:
        """Send once under the transport's at-most-once ladder.

        True only for a message the server accepted with a 250. A message
        that cannot be built (missing attachment, template rendered empty)
        fails here, before any connection.
        """
        try:
            msg = self._create_message(mailable_data)
            envelope_from = parseaddr(str(msg["From"] or ""))[1]
            recipients = self._envelope_recipients(mailable_data)
        except Exception as e:
            # A message we cannot even build will not build on a retry.
            self._log_error("SMTP message could not be built", e)
            return False

        try:
            outcome = send_smtp_envelope(
                msg,
                envelope_from=envelope_from,
                recipients=recipients,
                settings=self._settings(),
                category="cara.mail.smtp",
                max_attempts=self.max_attempts,
                retry_base_delay=self.retry_base_delay,
            )
        except SmtpAccountRefusedError as e:
            # Our own account (credentials, sending domain) was refused:
            # nothing was delivered and no retry fixes it.
            self._log_error("SMTP account refused", e)
            return False
        if outcome.status == MailDeliveryOutcome.UNKNOWN_DELIVERY:
            # Never True (it may not have landed), never retried (it may
            # have): the transport already logged the ambiguity.
            return False
        return outcome.sent

    def _settings(self) -> SmtpSettings:
        # Log in only with both halves, as this driver always has.
        username = self.username if (self.username and self.password) else ""
        return SmtpSettings(
            host=self.host,
            port=int(self.port),
            username=username or "",
            password=(self.password or "") if username else "",
            encryption=self.encryption,
            timeout=self.timeout,
        )

    @staticmethod
    def _envelope_recipients(data: dict[str, Any]) -> tuple[str, ...]:
        """Every To, Cc and Bcc address, bare (``"Name <a@b>"`` → ``a@b``).

        Bcc used to be dropped entirely: ``send_message`` only mails the
        addresses its headers name, and Bcc is (rightly) never a header.
        """
        values: list[str] = []
        for field in ("to", "cc", "bcc"):
            raw = data.get(field) or []
            values.extend([raw] if isinstance(raw, str) else [str(v) for v in raw])
        return tuple(address for _name, address in getaddresses(values)) or ("",)

    def _create_message(self, data: dict[str, Any]) -> Message:
        """
        Create email message from mailable data.

        ``multipart/alternative`` carries the bodies; with attachments it
        rides inside ``multipart/mixed`` (an attachment inside
        ``alternative`` is a third "version" of the body that clients hide).
        """
        html_content = data.get("html")
        if not html_content and data.get("view"):
            # The view was rendered by ``Mailable.to_dict`` and came back
            # empty: the mail lost the body it was written to carry. Fail
            # the send — never substitute a placeholder (the old one mailed
            # the raw ``view_data``, tokens included, to the recipient).
            raise ValueError(f"mail view {data.get('view')!r} rendered empty")

        body = MIMEMultipart("alternative")
        if data.get("text"):
            body.attach(MIMEText(data["text"], "plain", "utf-8"))
        if html_content:
            body.attach(MIMEText(html_content, "html", "utf-8"))

        attachments = data.get("attachments") or []
        if attachments:
            msg: Message = MIMEMultipart("mixed")
            msg.attach(body)
            for attachment in attachments:
                self._add_attachment(msg, attachment)
        else:
            msg = body

        # Set headers - use from address from mailable or driver config default
        from_address = data.get("from") or self.config.get("from_address")
        msg["From"] = strip_header_crlf(from_address)
        msg["To"] = strip_header_crlf(", ".join(data.get("to", [])))
        msg["Subject"] = strip_header_crlf(data.get("subject", ""))
        msg["Date"] = formatdate(usegmt=True)
        sender_domain = parseaddr(str(from_address or ""))[1].rpartition("@")[2]
        msg["Message-ID"] = make_msgid(domain=sender_domain or None)

        if data.get("cc"):
            msg["Cc"] = strip_header_crlf(", ".join(data.get("cc")))

        if data.get("reply_to"):
            msg["Reply-To"] = strip_header_crlf(data.get("reply_to"))

        # Set priority
        priority = data.get("priority", 3)
        if priority == 1:
            msg["X-Priority"] = "1 (Highest)"
            msg["X-MSMail-Priority"] = "High"
        elif priority == 5:
            msg["X-Priority"] = "5 (Lowest)"
            msg["X-MSMail-Priority"] = "Low"

        for name, value in (data.get("headers") or {}).items():
            header_name, header_value = validate_custom_header(name, value)
            del msg[header_name]
            msg[header_name] = header_value

        return msg

    @staticmethod
    def _add_attachment(msg: MIMEMultipart, attachment: dict[str, str]) -> None:
        """Attach one file, or raise — a mail promising an attachment it
        does not carry is not sent.

        The filename goes through ``add_header``'s parameter encoding
        (quoted; RFC 2231 when non-ASCII): the old literal
        ``filename= Q3 report.pdf`` parsed as ``Q3``.
        """
        file_path = attachment["path"]
        name = strip_header_crlf(attachment.get("name") or os.path.basename(file_path))
        mime_type, _encoding = mimetypes.guess_type(name)
        maintype, _, subtype = (mime_type or "application/octet-stream").partition("/")
        with open(file_path, "rb") as f:
            part = MIMEBase(maintype, subtype)
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename=name)
        msg.attach(part)

    def test_connection(self) -> bool:
        """
        Test SMTP connection.
        """
        try:
            server = open_smtp_connection(self._settings())
        except Exception as e:
            self._log_error("SMTP connection test failed", e)
            return False
        close_smtp(server)
        return True

    def _log_error(self, message: str, error: Exception) -> None:
        """Log SMTP errors via the framework logger with stderr fallback."""
        with contextlib.suppress(ImportError, RuntimeError):
            Log.error("%s: %s", message, error, category="cara.mail.smtp", exc_info=True)
