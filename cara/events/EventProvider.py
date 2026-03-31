"""
Event Provider for the Cara framework.

This module provides the service provider that registers event handling and dispatching into the
application.
"""

from typing import List
from cara.foundation import DeferredProvider
from cara.events import Event


class EventProvider(DeferredProvider):
    """Deferred provider that binds Event under "events" key."""

    @classmethod
    def provides(cls) -> List[str]:
        return ["events"]

    def register(self) -> None:
        """
        Instantiate Event and bind to container.

        Now `app.make("events")` returns Event instance.
        """
        self.application.bind("events", Event())
