"""
Event Contract for the Cara framework.

This module defines the contract that any event class must implement, specifying required methods
for event handling.
"""

from typing import Any, Dict, Protocol


class Event(Protocol):
    """Protocol that every Event class must fulfill."""

    @classmethod
    def name(self) -> str:
        """Return a unique string name for this event."""

    def payload(self) -> Dict[str, Any]:
        """Return a dict of any data carried by this event."""
