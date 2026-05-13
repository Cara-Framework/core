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

    def meta(self, key: str, default: Any = None) -> Any:
        """Read a value from the event's metadata dict.

        Consolidates the 5 different null-guard patterns listeners used::

            # Before (inconsistent across listeners):
            priority = (event.metadata or {}).get("priority", "default")
            priority = event.metadata.get("priority", "default") if hasattr(event, "metadata") and event.metadata else "default"

            # After:
            priority = event.meta("priority", "default")
        """
        md = getattr(self, "metadata", None) or {}
        return md.get(key, default)
