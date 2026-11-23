"""SMTP constants and header-safety helpers.

Project-agnostic framework utility: scrub user-influenced values before they
land in SMTP headers (CRLF-injection defence) and provide sane SMTP defaults.
"""

from __future__ import annotations

DEFAULT_SMTP_PORT = 587
SMTP_TIMEOUT_SECONDS = 10


# Every character ``email.header`` splits a header value on, not only CR/LF:
# it folds with ``str.splitlines``-style ASCII breaks, so a vertical tab, form
# feed or FS/GS/RS in a subject left "an embedded header" behind and the send
# raised ``HeaderParseError`` — the opposite of what this scrub promises.
_HEADER_LINE_BREAKS = str.maketrans(dict.fromkeys("\r\n\x0b\x0c\x1c\x1d\x1e", " "))


def strip_header_crlf(value: object) -> str:
    """Collapse every line break in an SMTP header value to a single space.

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
    return value.translate(_HEADER_LINE_BREAKS)


__all__ = [
    "DEFAULT_SMTP_PORT",
    "SMTP_TIMEOUT_SECONDS",
    "strip_header_crlf",
]
