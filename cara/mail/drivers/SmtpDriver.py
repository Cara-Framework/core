"""
SMTP Mail Driver for Cara Framework.

This module provides SMTP email sending functionality following
Cara framework conventions.
"""

import os
import smtplib
import ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, Optional

from cara.mail.contracts import Mail


class SmtpDriver(Mail):
    driver_name = "smtp"

    def __init__(self, config: Dict[str, Any]):
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
        self.encryption = config.get("encryption", "tls")  # tls, ssl, none
        self.timeout = config.get("timeout", 30)

    def send(self, mailable_data: Dict[str, Any]) -> bool:
        """
        Send email using SMTP.
        """
        try:
            # Create message
            msg = self._create_message(mailable_data)

            # Connect and send
            with self._get_connection() as server:
                if self.username and self.password:
                    server.login(self.username, self.password)

                # Send email
                server.send_message(msg)
                return True

        except Exception as e:
            # Log error (could integrate with Cara's logging system)
            print(f"SMTP Error: {e}")
            return False

    def _create_message(self, data: Dict[str, Any]) -> MIMEMultipart:
        """
        Create email message from mailable data.
        """
        msg = MIMEMultipart("alternative")

        # Set headers - use from address from mailable or driver config default
        from_address = data.get("from")
        if not from_address:
            from_address = self.config.get("from_address")
        msg["From"] = from_address
        msg["To"] = ", ".join(data.get("to", []))
        msg["Subject"] = data.get("subject", "")

        if data.get("cc"):
            msg["Cc"] = ", ".join(data.get("cc"))

        if data.get("reply_to"):
            msg["Reply-To"] = data.get("reply_to")

        # Set priority
        priority = data.get("priority", 3)
        if priority == 1:
            msg["X-Priority"] = "1 (Highest)"
            msg["X-MSMail-Priority"] = "High"
        elif priority == 5:
            msg["X-Priority"] = "5 (Lowest)"
            msg["X-MSMail-Priority"] = "Low"

        # Add text content
        if data.get("text"):
            text_part = MIMEText(data["text"], "plain", "utf-8")
            msg.attach(text_part)

        # Add HTML content
        html_content = data.get("html")
        if not html_content and data.get("view"):
            # Render view if available
            html_content = self._render_view(data.get("view"), data.get("view_data", {}))

        if html_content:
            html_part = MIMEText(html_content, "html", "utf-8")
            msg.attach(html_part)

        # Add attachments
        for attachment in data.get("attachments", []):
            self._add_attachment(msg, attachment)

        return msg

    def _render_view(self, template: str, data: Dict[str, Any]) -> Optional[str]:
        """
        Render view template. This is a placeholder implementation.
        """
        # This would integrate with Cara's view system
        # For now, return a simple template
        return (
            f"<html><body><h1>Email Template: {template}</h1><p>{data}</p></body></html>"
        )

    def _add_attachment(self, msg: MIMEMultipart, attachment: Dict[str, str]) -> None:
        """
        Add file attachment to message.
        """
        try:
            file_path = attachment["path"]
            if not os.path.exists(file_path):
                return

            with open(file_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())

            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition", f"attachment; filename= {attachment['name']}"
            )
            msg.attach(part)

        except Exception as e:
            print(f"Attachment error: {e}")

    def _get_connection(self) -> smtplib.SMTP:
        """
        Get SMTP connection based on encryption settings.
        """
        if self.encryption == "ssl":
            context = ssl.create_default_context()
            server = smtplib.SMTP_SSL(
                self.host, self.port, timeout=self.timeout, context=context
            )
        else:
            server = smtplib.SMTP(self.host, self.port, timeout=self.timeout)

            if self.encryption == "tls":
                context = ssl.create_default_context()
                server.starttls(context=context)

        return server

    def test_connection(self) -> bool:
        """
        Test SMTP connection.
        """
        try:
            with self._get_connection() as server:
                if self.username and self.password:
                    server.login(self.username, self.password)
                return True
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
