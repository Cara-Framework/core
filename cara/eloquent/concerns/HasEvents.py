"""
HasEvents Concern

Single Responsibility: Handle model events for Eloquent models.
Clean separation of event logic following SRP.
"""

from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from cara.eloquent.models.Model import Model


class HasEvents:
    """
    Mixin for handling model events.

    This concern handles:
    - Model lifecycle events (creating, created, saving, saved, etc.)
    - Event firing and cancellation
    - Event observer management
    """

    __has_events__ = True
    __observers__ = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # ===== Event Firing =====

    def _fire_model_event(self, event_name: str, **kwargs) -> bool:
        """
        Fire a model event and return whether it was cancelled.

        Args:
            event_name: Name of the event to fire
            **kwargs: Additional data to pass to event handlers

        Returns:
            True if event should continue, False if cancelled
        """
        if not self.__has_events__:
            return True

        # Get event methods from model
        event_methods = self._get_model_events()

        if event_name in event_methods:
            for method in event_methods[event_name]:
                try:
                    result = method(self, **kwargs)
                    # If any handler returns False, cancel the operation
                    if result is False:
                        return False
                except Exception:
                    # Log the error but don't stop other handlers
                    pass

        # Fire global observers
        return self._fire_observers(event_name, **kwargs)

    def _fire_observers(self, event_name: str, **kwargs) -> bool:
        """Fire registered observers for an event."""
        observers = self.__observers__.get(self.__class__, {})

        if event_name in observers:
            for observer in observers[event_name]:
                try:
                    result = observer(self, **kwargs)
                    if result is False:
                        return False
                except Exception:
                    pass

        return True

    def _get_model_events(self) -> Dict[str, list]:
        """Get all event methods defined on the model."""
        events = {
            "creating": [],
            "created": [],
            "saving": [],
            "saved": [],
            "updating": [],
            "updated": [],
            "deleting": [],
            "deleted": [],
        }

        # Look for event decorator methods
        for attr_name in dir(self):
            attr = getattr(self, attr_name)

            # Check for event decorators
            if hasattr(attr, "_event_type"):
                event_type = attr._event_type
                if event_type in events:
                    events[event_type].append(attr)

        return events

    # ===== Event Management =====

    @classmethod
    def observe(cls, observer: "Model") -> None:
        """Register an observer for this model."""
        if cls not in cls.__observers__:
            cls.__observers__[cls] = {}

        # Register observer methods
        for event_name in [
            "creating",
            "created",
            "saving",
            "saved",
            "updating",
            "updated",
            "deleting",
            "deleted",
        ]:
            method_name = event_name
            if hasattr(observer, method_name):
                method = getattr(observer, method_name)
                if callable(method):
                    if event_name not in cls.__observers__[cls]:
                        cls.__observers__[cls][event_name] = []
                    cls.__observers__[cls][event_name].append(method)

    @classmethod
    def without_events(cls) -> "HasEvents":
        """Disable events for subsequent operations."""
        instance = cls()
        instance.__has_events__ = False
        return instance

    @classmethod
    def with_events(cls) -> "HasEvents":
        """Enable events for subsequent operations."""
        instance = cls()
        instance.__has_events__ = True
        return instance

    def disable_events(self) -> "HasEvents":
        """Disable events on this instance."""
        self.__has_events__ = False
        return self

    def enable_events(self) -> "HasEvents":
        """Enable events on this instance."""
        self.__has_events__ = True
        return self

    # ===== Silent Operations =====

    def save_quietly(self, **kwargs) -> bool:
        """Save the model without firing events."""
        original_events = self.__has_events__
        self.__has_events__ = False

        try:
            result = self.save(**kwargs)
        finally:
            self.__has_events__ = original_events

        return result

    def delete_quietly(self, **kwargs) -> bool:
        """Delete the model without firing events."""
        original_events = self.__has_events__
        self.__has_events__ = False

        try:
            result = self.delete(**kwargs)
        finally:
            self.__has_events__ = original_events

        return result

    def update_quietly(self, attributes: Dict[str, Any], **kwargs) -> bool:
        """Update the model without firing events."""
        original_events = self.__has_events__
        self.__has_events__ = False

        try:
            for key, value in attributes.items():
                setattr(self, key, value)
            result = self.save(**kwargs)
        finally:
            self.__has_events__ = original_events

        return result
