"""
Mailable Class for Cara Framework.

This module provides the base Mailable class for creating and sending emails,
following Laravel-style API with Cara framework conventions.
Includes automatic serialization support for queue jobs.
"""

from __future__ import annotations

import os
import re
from typing import Any

from cara.queues.contracts import SerializesModels

_HEADER_NAME = re.compile(r"^[A-Za-z0-9!#$%&'*+\-.^_`|~]+$")
_PROTECTED_HEADERS = frozenset(
    {
        "authentication-results",
        "bcc",
        "cc",
        "content-transfer-encoding",
        "content-type",
        "date",
        "dkim-signature",
        "from",
        "message-id",
        "mime-version",
        "received",
        "reply-to",
        "return-path",
        "sender",
        "subject",
        "to",
    }
)


def validate_custom_header(name: str, value: str) -> tuple[str, str]:
    """Validate an application-owned header without permitting injection."""
    if not isinstance(name, str) or not _HEADER_NAME.fullmatch(name):
        raise ValueError("Invalid mail header name")
    normalized_name = name.strip()
    lowered = normalized_name.lower()
    if lowered in _PROTECTED_HEADERS or lowered.startswith("arc-"):
        raise ValueError(f"Mail header is framework-managed: {normalized_name}")
    if not isinstance(value, str):
        raise TypeError("Mail header value must be a string")
    normalized_value = value.strip()
    if not normalized_value or any(char in normalized_value for char in "\r\n\x00"):
        raise ValueError("Invalid mail header value")
    if len(normalized_value) > 998:
        raise ValueError("Mail header value exceeds RFC line length")
    return normalized_name, normalized_value


class Mailable(SerializesModels):
    def __init__(self):
        """Initialize mailable with default values."""
        super().__init__()
        self._to: list[str] = []
        self._from: str | None = None
        self._cc: list[str] = []
        self._bcc: list[str] = []
        self._subject: str | None = None
        self._reply_to: str | None = None
        self._text: str | None = None
        self._html: str | None = None
        self._view: str | None = None
        self._view_data: dict[str, Any] = {}
        self._priority: int = 3  # Normal priority (1-5)
        self._attachments: list[dict[str, str]] = []
        # to_dict() emits ``"headers": self._headers``; without this init
        # every send (sync Mail._send_now and queued SendMailableJob)
        # raised AttributeError before any driver ran.
        self._headers: dict[str, str] = {}
        self._application = None

    def to(self, address: str | list[str]) -> Mailable:
        """
        Set the recipient address(es).
        """
        if isinstance(address, str):
            self._to.append(address)
        else:
            self._to.extend(address)
        return self

    def from_(self, address: str) -> Mailable:
        """
        Set the sender address.
        """
        self._from = address
        return self

    def cc(self, addresses: str | list[str]) -> Mailable:
        """
        Set CC addresses.
        """
        if isinstance(addresses, str):
            self._cc.append(addresses)
        else:
            self._cc.extend(addresses)
        return self

    def bcc(self, addresses: str | list[str]) -> Mailable:
        """
        Set BCC addresses.
        """
        if isinstance(addresses, str):
            self._bcc.append(addresses)
        else:
            self._bcc.extend(addresses)
        return self

    def subject(self, subject: str) -> Mailable:
        """
        Set the email subject.
        """
        self._subject = subject
        return self

    def reply_to(self, address: str) -> Mailable:
        """
        Set the reply-to address.
        """
        self._reply_to = address
        return self

    def text(self, content: str) -> Mailable:
        """
        Set the plain text content.
        """
        self._text = content
        return self

    def html(self, content: str) -> Mailable:
        """
        Set the HTML content.
        """
        self._html = content
        return self

    def view(self, template: str, data: dict[str, Any] = None) -> Mailable:
        """
        Set the view template for the email.
        """
        self._view = template
        self._view_data = data or {}
        return self

    def priority(self, level: int) -> Mailable:
        """
        Set the email priority.
        """
        self._priority = max(1, min(5, level))
        return self

    def header(self, name: str, value: str) -> Mailable:
        """Set one validated custom message header."""
        normalized_name, normalized_value = validate_custom_header(name, value)
        self._headers[normalized_name] = normalized_value
        return self

    def headers(self, values: dict[str, str]) -> Mailable:
        """Set validated custom message headers."""
        for name, value in values.items():
            self.header(name, value)
        return self

    def high_priority(self) -> Mailable:
        """
        Set high priority (1).
        """
        return self.priority(1)

    def low_priority(self) -> Mailable:
        """
        Set low priority (5).
        """
        return self.priority(5)

    def attach(self, name: str, path: str) -> Mailable:
        """
        Attach a file to the email.
        """
        if os.path.exists(path):
            self._attachments.append({"name": name, "path": path})
        return self

    def set_application(self, application) -> Mailable:
        """
        Set the application instance (for template rendering).
        """
        self._application = application
        return self

    def build(self) -> Mailable:
        """
        Build the mailable. Override this method in subclasses.
        """
        return self

    def to_dict(self):
        """Convert the mailable to a dictionary for queue serialization."""
        html_content = self.render_html()

        result = {
            "to": self._to,
            "from": self._from,
            # ``reply_to`` / ``cc`` / ``bcc`` / ``priority`` were set on the
            # Mailable but never emitted here — so every driver (which reads
            # them via ``data.get(...)``) and the queued ``SendMailableJob``
            # (which round-trips through ``to_dict``) silently dropped them.
            # ``Mail.to(x).cc(y).reply_to(z).send()`` lost cc + reply_to with
            # no error. Emit them so what the caller sets is what the driver
            # transmits.
            "reply_to": self._reply_to,
            "cc": self._cc,
            "bcc": self._bcc,
            "priority": self._priority,
            "subject": self._subject,
            "html": html_content,
            "text": self._text,
            "attachments": self._attachments,
            "headers": self._headers,
            "view": self._view,
            "view_data": self._view_data,
        }

        return result

    def render_html(self):
        """Render the HTML body.

        Mail templates are standard Jinja2 (filters, ``is defined`` tests,
        ``{% for %}`` loops) and render through :mod:`cara.mail.JinjaRenderer`.
        A missing template or template error raises — mail rendering fails loud
        rather than silently shipping an empty body.
        """
        if self._html:
            return self._html
        if not self._view:
            return None

        from cara.mail.JinjaRenderer import render_mail_view

        return render_mail_view(self._application, self._view, self._view_data or {})

    def __str__(self) -> str:
        """String representation for debugging."""
        return f"<Mailable to={self._to} subject='{self._subject}'>"
