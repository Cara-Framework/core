"""
Mailgun driver for Cara Framework mail system.

This driver sends emails using Mailgun API service,
supporting all Mailgun features including regions and attachments.
"""

import os
from typing import Any, Dict, List, Optional

import requests

from cara.mail.contracts import Mail


class MailgunDriver(Mail):
    """Mailgun driver for sending emails via Mailgun API."""

    driver_name = "mailgun"

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Mailgun driver.

        Args:
            config: Mailgun configuration
        """
        self.config = config
        self.secret = config.get("secret")
        self.domain = config.get("domain")
        self.region = config.get("region", "us")
        self.endpoint = self._get_endpoint()

        if not self.secret or not self.domain:
            raise ValueError("Mailgun driver requires 'secret' and 'domain' in config")

    def _get_endpoint(self) -> str:
        """Get Mailgun API endpoint based on region."""
        endpoints = {
            "us": "https://api.mailgun.net/v3",
            "eu": "https://api.eu.mailgun.net/v3",
        }
        return endpoints.get(self.region.lower(), endpoints["us"])

    def send(self, mailable_data: Dict[str, Any]) -> bool:
        """
        Send email using Mailgun API.

        Args:
            mailable_data: Dictionary containing email data

        Returns:
            True if email sent successfully, False otherwise
        """
        try:
            # Prepare data
            data = self._prepare_data(mailable_data)
            files = self._prepare_attachments(mailable_data)

            # Send request
            response = requests.post(
                f"{self.endpoint}/{self.domain}/messages",
                auth=("api", self.secret),
                data=data,
                files=files,
                timeout=self.config.get("timeout", 30),
            )

            # Close file handles
            for file_tuple in files:
                if len(file_tuple) > 1 and hasattr(file_tuple[1], "close"):
                    file_tuple[1].close()

            # Check response
            if response.status_code == 200:
                return True
            else:
                print(f"Mailgun Error: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            print(f"Mailgun Error: {str(e)}")
            return False

    def _prepare_data(self, mailable_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare email data for Mailgun API.

        Args:
            mailable_data: Mailable data dictionary

        Returns:
            Formatted data for Mailgun API
        """
        data = {}

        # Required fields - use from address from mailable or driver config default
        from_address = mailable_data.get("from")
        if not from_address:
            from_address = self.config.get("from_address")
        data["from"] = from_address
        data["to"] = self._format_recipients(mailable_data.get("to", []))
        data["subject"] = mailable_data.get("subject", "")

        # Optional fields
        if mailable_data.get("cc"):
            data["cc"] = self._format_recipients(mailable_data.get("cc"))

        if mailable_data.get("bcc"):
            data["bcc"] = self._format_recipients(mailable_data.get("bcc"))

        if mailable_data.get("reply_to"):
            data["h:Reply-To"] = mailable_data.get("reply_to")

        # Content
        if mailable_data.get("text"):
            data["text"] = mailable_data.get("text")

        html_content = mailable_data.get("html")
        if not html_content and mailable_data.get("view"):
            # Render view if available
            html_content = self._render_view(
                mailable_data.get("view"), mailable_data.get("view_data", {})
            )

        if html_content:
            data["html"] = html_content

        # Priority
        priority = mailable_data.get("priority")
        if priority:
            data["h:X-Priority"] = str(priority)

        # Tags (Mailgun feature)
        if self.config.get("tags"):
            for tag in self.config.get("tags"):
                data["o:tag"] = tag

        # Tracking (Mailgun features)
        if self.config.get("track_clicks") is not None:
            data["o:tracking-clicks"] = "yes" if self.config.get("track_clicks") else "no"

        if self.config.get("track_opens") is not None:
            data["o:tracking-opens"] = "yes" if self.config.get("track_opens") else "no"

        return data

    def _format_recipients(self, recipients: List[str]) -> str:
        """
        Format recipient list for Mailgun.

        Args:
            recipients: List of email addresses

        Returns:
            Comma-separated string of recipients
        """
        if isinstance(recipients, str):
            return recipients
        return ", ".join(recipients)

    def _prepare_attachments(self, mailable_data: Dict[str, Any]) -> List[tuple]:
        """
        Prepare attachments for Mailgun.

        Args:
            mailable_data: Mailable data dictionary

        Returns:
            List of file tuples for requests
        """
        files = []
        attachments = mailable_data.get("attachments", [])

        for attachment in attachments:
            file_path = attachment.get("path")
            file_name = attachment.get("name")

            if file_path and os.path.exists(file_path):
                try:
                    file_handle = open(file_path, "rb")
                    files.append(("attachment", (file_name, file_handle)))
                except Exception as e:
                    print(f"Attachment error for {file_path}: {e}")

        return files

    def _render_view(self, template: str, data: Dict[str, Any]) -> Optional[str]:
        """
        Render view template. This is a placeholder implementation.

        Args:
            template: Template name
            data: Template data

        Returns:
            Rendered HTML or None
        """
        # This would integrate with Cara's view system
        return (
            f"<html><body><h1>Email Template: {template}</h1><p>{data}</p></body></html>"
        )

    def test_connection(self) -> bool:
        """
        Test Mailgun connection by validating domain.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            response = requests.get(
                f"{self.endpoint}/domains/{self.domain}",
                auth=("api", self.secret),
                timeout=10,
            )
            return response.status_code == 200
        except Exception as e:
            print(f"Mailgun connection test failed: {e}")
            return False
