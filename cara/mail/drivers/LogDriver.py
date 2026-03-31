"""
Log driver for Cara Framework mail system.

This driver logs emails instead of sending them,
useful for development and testing.
"""

from typing import Any, Dict

from cara.mail.contracts import Mail


class LogDriver(Mail):
    driver_name = "log"

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize log driver.

        Args:
            config: Driver configuration
        """
        self.config = config

    def send(self, mailable_data: Dict[str, Any]) -> bool:
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

        # Log email details
        try:
            from cara.facades import Logger

            Logger.debug("=== EMAIL LOG ===")
            Logger.debug(f"To: {mailable_data.get('to')}")
            Logger.debug(f"From: {from_address}")
            Logger.debug(f"Subject: {mailable_data.get('subject')}")
            Logger.debug(
                f"Content: {mailable_data.get('text') or mailable_data.get('html')}"
            )
            Logger.debug("=================")
        except ImportError:
            # Fallback to print if logger not available
            print("=== EMAIL LOG ===")
            print(f"To: {mailable_data.get('to')}")
            print(f"From: {from_address}")
            print(f"Subject: {mailable_data.get('subject')}")
            print(f"Content: {mailable_data.get('text') or mailable_data.get('html')}")
            print("=================")

        return True
