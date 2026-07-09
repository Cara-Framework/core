"""
Log driver for Cara Framework mail system.

This driver logs emails instead of sending them,
useful for development and testing.
"""

from __future__ import annotations

from typing import Any

from cara.mail.contracts import Mail


class LogDriver(Mail):
    driver_name = "log"

    def __init__(self, config: dict[str, Any]):
        """
        Initialize log driver.

        Args:
            config: Driver configuration
        """
        self.config = config

    def send(self, mailable_data: dict[str, Any]) -> bool:
        """
        Log email instead of sending.

        Args:
            mailable_data: Email data to log

        Returns:
            True (always successful for logging)
        """
        # Use from address from mailable or driver config default
        from_address = mailable_data.get("from")
        if not from_address:
            from_address = self.config.get("from_address")

        # Log email details — redact recipient addresses and truncate
        # body content so PII and full message text never land in log
        # aggregation, even in dev/test environments.
        try:
            from cara.facades import Logger
            from cara.support.Str import email_mask

            to_raw = mailable_data.get("to")
            to_masked = email_mask(to_raw) if isinstance(to_raw, str) else str(to_raw)
            from_masked = email_mask(from_address) if isinstance(from_address, str) else str(from_address)
            content_preview = (mailable_data.get("text") or mailable_data.get("html") or "")[:80]

            Logger.debug("=== EMAIL LOG ===")
            Logger.debug("To: %s", to_masked)
            Logger.debug("From: %s", from_masked)
            Logger.debug("Subject: %s", mailable_data.get("subject"))
            Logger.debug("Content: %s...", content_preview)
            Logger.debug("=================")
        except ImportError:
            # Fallback — still redact; never log raw PII.
            print("=== EMAIL LOG ===")
            print("To: [REDACTED]")
            print("From: [REDACTED]")
            print(f"Subject: {mailable_data.get('subject')}")
            print("Content: [REDACTED]")
            print("=================")

        return True
