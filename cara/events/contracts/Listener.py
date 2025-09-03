"""
Listener Contract for the Cara framework.

This module defines the contract that any event listener must implement, specifying required methods
for event listening.
"""

from typing import Protocol

from .Event import Event


class Listener(Protocol):
    """Protocol that each Listener must implement."""

    def handle(self, event: Event) -> None:
        """Receive an Event instance and perform any logic."""
