"""
Mail Interface for the Cara framework.

This module defines the contract that any mail driver must implement, specifying required methods
for email operations.
"""

from typing import Any, Dict, Protocol


class Mail(Protocol):
    """Contract that any mail driver must implement."""

    def send(self, mailable_data: Dict[str, Any]) -> bool:
        """
        Send email using the driver's implementation.

        Args:
            mailable_data: Email data containing recipients, subject, content, etc.

        Returns:
            True if email was sent successfully, False otherwise
        """
        ...
