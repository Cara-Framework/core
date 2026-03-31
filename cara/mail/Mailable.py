"""
Mailable Class for Cara Framework.

This module provides the base Mailable class for creating and sending emails,
following Laravel-style API with Cara framework conventions.
Includes automatic serialization support for queue jobs.
"""

import os
from typing import Any, Dict, List, Optional, Union

from cara.queues.contracts import SerializesModels


class Mailable(SerializesModels):
    def __init__(self):
        """Initialize mailable with default values."""
        super().__init__()
        self._to: List[str] = []
        self._from: Optional[str] = None
        self._cc: List[str] = []
        self._bcc: List[str] = []
        self._subject: Optional[str] = None
        self._reply_to: Optional[str] = None
        self._text: Optional[str] = None
        self._html: Optional[str] = None
        self._view: Optional[str] = None
        self._view_data: Dict[str, Any] = {}
        self._priority: int = 3  # Normal priority (1-5)
        self._attachments: List[Dict[str, str]] = []
        self._application = None

    def to(self, address: Union[str, List[str]]) -> "Mailable":
        """
        Set the recipient address(es).
        """
        if isinstance(address, str):
            self._to.append(address)
        else:
            self._to.extend(address)
        return self

    def from_(self, address: str) -> "Mailable":
        """
        Set the sender address.
        """
        self._from = address
        return self

    def cc(self, addresses: Union[str, List[str]]) -> "Mailable":
        """
        Set CC addresses.
        """
        if isinstance(addresses, str):
            self._cc.append(addresses)
        else:
            self._cc.extend(addresses)
        return self

    def bcc(self, addresses: Union[str, List[str]]) -> "Mailable":
        """
        Set BCC addresses.
        """
        if isinstance(addresses, str):
            self._bcc.append(addresses)
        else:
            self._bcc.extend(addresses)
        return self

    def subject(self, subject: str) -> "Mailable":
        """
        Set the email subject.
        """
        self._subject = subject
        return self

    def reply_to(self, address: str) -> "Mailable":
        """
        Set the reply-to address.
        """
        self._reply_to = address
        return self

    def text(self, content: str) -> "Mailable":
        """
        Set the plain text content.
        """
        self._text = content
        return self

    def html(self, content: str) -> "Mailable":
        """
        Set the HTML content.
        """
        self._html = content
        return self

    def view(self, template: str, data: Dict[str, Any] = None) -> "Mailable":
        """
        Set the view template for the email.
        """
        self._view = template
        self._view_data = data or {}
        return self

    def priority(self, level: int) -> "Mailable":
        """
        Set the email priority.
        """
        self._priority = max(1, min(5, level))
        return self

    def high_priority(self) -> "Mailable":
        """
        Set high priority (1).
        """
        return self.priority(1)

    def low_priority(self) -> "Mailable":
        """
        Set low priority (5).
        """
        return self.priority(5)

    def attach(self, name: str, path: str) -> "Mailable":
        """
        Attach a file to the email.
        """
        if os.path.exists(path):
            self._attachments.append({"name": name, "path": path})
        return self

    def set_application(self, application) -> "Mailable":
        """
        Set the application instance (for template rendering).
        """
        self._application = application
        return self

    def build(self) -> "Mailable":
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
        """Render the HTML content for the email."""
        if self._html:
            return self._html

        if self._view:
            try:
                # Try View facade first
                from cara.facades import View

                result = View.render(self._view, self._view_data or {})
                return result
            except ImportError:
                pass
            except Exception:
                pass

            # Try application view service
            try:
                view_service = self._application.make("view")
                result = view_service.render(self._view, self._view_data or {})
                return result
            except Exception:
                pass

        return None

    def __str__(self) -> str:
        """String representation for debugging."""
        return f"<Mailable to={self._to} subject='{self._subject}'>"
