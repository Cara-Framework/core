"""
Array driver for Cara Framework mail system.

This driver discards emails; it exists so a dev/test ``MAIL_DRIVER=array``
resolves to a real transport object instead of failing. Nothing reads what
it "sends" — test assertions belong on ``MailFake`` (``assert_sent``), and
``MailProvider`` refuses this driver outright in production.
"""

from __future__ import annotations

from typing import Any

from cara.mail.contracts import MailContract


class ArrayDriver(MailContract):
    driver_name = "array"

    def __init__(self, config: dict[str, Any]):
        """
        Initialize array driver.
        """
        self.config = config

    def send(self, mailable_data: dict[str, Any]) -> bool:
        """Discard the email and report success."""
        return True
