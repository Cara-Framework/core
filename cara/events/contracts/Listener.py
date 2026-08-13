"""
Listener Contract for the Cara framework.

This module defines the contract that any event listener must implement, specifying required methods
for event listening.
"""

from __future__ import annotations

from typing import Protocol

from .EventContract import EventContract


class Listener(Protocol):
    """Protocol that each Listener must implement."""

    def handle(self, event: EventContract) -> None:
        """Receive an Event instance and perform any logic."""
