"""
Array driver for Cara Framework mail system.

This driver stores emails in memory for testing purposes.
"""

from __future__ import annotations

from typing import Any

from cara.mail.contracts import Mail


class ArrayDriver(Mail):
    driver_name = "array"

    def __init__(self, config: dict[str, Any]):
        """
        Initialize array driver.
        """
        self.config = config
        self.sent_mails: list[dict[str, Any]] = []

    def send(self, mailable_data: dict[str, Any]) -> bool:
        """
        Store email in array for testing.
        """
        # Use from address from mailable or driver config default
        email_data = mailable_data.copy()
        from_address = email_data.get("from")
        if not from_address:
            from_address = self.config.get("from_address")
        email_data["from"] = from_address

        self.sent_mails.append(email_data)
        return True

    def get_sent_mails(self) -> list[dict[str, Any]]:
        """
        Get sent mails for testing.
        """
        return self.sent_mails

    def clear(self) -> None:
        """Clear sent mails."""
        self.sent_mails = []
