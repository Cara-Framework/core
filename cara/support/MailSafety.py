"""SMTP constants and header-safety helpers.

Project-agnostic framework utility: scrub user-influenced values before they
land in SMTP headers (CRLF-injection defence) and provide sane SMTP defaults.
"""

from __future__ import annotations

DEFAULT_SMTP_PORT = 587
SMTP_TIMEOUT_SECONDS = 10


def strip_header_crlf(value: object) -> str:
    """Collapse CR / LF in SMTP header values to a single space.

    ``MIMEMultipart`` defaults to the ``compat32`` policy, which assigns
    header values literally.  A CR or LF in user-influenced content
    (``to_email``, ``from_name``, the subject line) lets an attacker inject a
    second SMTP header.  Scrub at the boundary so the message sends with safe
    headers instead of raising.

    Non-string inputs collapse to an empty string so a stray ``None`` doesn't
    500 the helper.
    """
    if not isinstance(value, str):
        return ""
    return value.replace("\r", " ").replace("\n", " ")


__all__ = [
    "DEFAULT_SMTP_PORT",
    "SMTP_TIMEOUT_SECONDS",
    "strip_header_crlf",
]
