"""Canonical definition of ``EventSubscriber``."""

from __future__ import annotations

from typing import Any


class EventSubscriber:
    """
    Base class for event subscribers.

    Group related event listeners together by extending this class
    and implementing the subscribe() method.

    Example:
        class UserEventSubscriber(EventSubscriber):
            def subscribe(self, dispatcher):
                dispatcher.subscribe('user.created', UserCreatedListener())
                dispatcher.subscribe('user.updated', UserUpdatedListener())

            def on_user_created(self, event):
                # Handle user created
                pass

            def on_user_updated(self, event):
                # Handle user updated
                pass
    """

    def subscribe(self, dispatcher: Any) -> None:
        """
        Subscribe to events in the dispatcher.

        Args:
            dispatcher: The Event dispatcher instance
        """
        raise NotImplementedError("Subscriber must implement subscribe() method")
