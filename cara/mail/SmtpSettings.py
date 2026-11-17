"""``SmtpSettings``: where and how ``SmtpTransport`` opens its connection."""

from __future__ import annotations

from dataclasses import dataclass

from cara.support import DEFAULT_SMTP_PORT, SMTP_TIMEOUT_SECONDS


@dataclass(frozen=True, slots=True)
class SmtpSettings:
    """Where and how to open the SMTP connection.

    ``encryption`` is ``tls`` (STARTTLS, the default), ``ssl`` (implicit
    TLS) or ``none``. Both TLS forms verify the server certificate.
    """

    host: str
    port: int = DEFAULT_SMTP_PORT
    username: str = ""
    password: str = ""
    encryption: str = "tls"
    timeout: float = SMTP_TIMEOUT_SECONDS


__all__ = ["SmtpSettings"]
