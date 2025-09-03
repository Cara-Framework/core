"""
Model Events Decorators for Laravel-style model lifecycle hooks.

Provides decorators for registering model event listeners that fire automatically
during model operations like save, create, update, delete.

Example:
    class User(Model):
        @creating
        def generate_uuid(self):
            self.uuid = str(uuid.uuid4())

        @saving
        def update_timestamp(self):
            self.updated_at = datetime.now()

        @deleting
        def check_permissions(self):
            if not self.can_delete:
                return False  # Cancel delete operation

Usage:
    user = User(name="John")
    user.save()  # Triggers: creating -> saving -> created -> saved
"""

import functools
from typing import Callable


def _register_model_event(event_name: str, method: Callable) -> Callable:
    """
    Register a method as a model event listener.

    Args:
        event_name (str): The name of the event (e.g., 'creating', 'saving')
        method (Callable): The method to register as an event listener

    Returns:
        Callable: The decorated method with event metadata
    """
    # Mark the method as a model event listener
    method._is_model_event = True
    method._event_name = event_name

    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        return method(*args, **kwargs)

    # Copy event metadata to wrapper
    wrapper._is_model_event = True
    wrapper._event_name = event_name

    return wrapper


def creating(method: Callable) -> Callable:
    """
    Decorator for 'creating' event - fires before a new model is saved for the first time.

    Example:
        @creating
        def generate_uuid(self):
            self.uuid = str(uuid.uuid4())
    """
    return _register_model_event("creating", method)


def created(method: Callable) -> Callable:
    """
    Decorator for 'created' event - fires after a new model is saved for the first time.

    Example:
        @created
        def send_welcome_email(self):
            self.send_email('welcome')
    """
    return _register_model_event("created", method)


def updating(method: Callable) -> Callable:
    """
    Decorator for 'updating' event - fires before an existing model is updated.

    Example:
        @updating
        def log_changes(self):
            self.change_log.append(self.get_dirty())
    """
    return _register_model_event("updating", method)


def updated(method: Callable) -> Callable:
    """
    Decorator for 'updated' event - fires after an existing model is updated.

    Example:
        @updated
        def clear_cache(self):
            cache.forget(f"user_{self.id}")
    """
    return _register_model_event("updated", method)


def saving(method: Callable) -> Callable:
    """
    Decorator for 'saving' event - fires before any save operation (create or update).

    Example:
        @saving
        def validate_data(self):
            if not self.email:
                raise ValueError("Email is required")
    """
    return _register_model_event("saving", method)


def saved(method: Callable) -> Callable:
    """
    Decorator for 'saved' event - fires after any save operation (create or update).

    Example:
        @saved
        def update_search_index(self):
            SearchIndex.update(self)
    """
    return _register_model_event("saved", method)


def deleting(method: Callable) -> Callable:
    """
    Decorator for 'deleting' event - fires before a model is deleted.

    Return False to cancel the delete operation.

    Example:
        @deleting
        def prevent_admin_delete(self):
            if self.role == 'admin':
                return False  # Cancel delete
    """
    return _register_model_event("deleting", method)


def deleted(method: Callable) -> Callable:
    """
    Decorator for 'deleted' event - fires after a model is deleted.

    Example:
        @deleted
        def cleanup_related_data(self):
            self.posts().delete()
    """
    return _register_model_event("deleted", method)


# Event execution order constants
EVENT_ORDER = {
    "save_new": ["creating", "saving", "created", "saved"],
    "save_existing": ["updating", "saving", "updated", "saved"],
    "delete": ["deleting", "deleted"],
}


def get_model_events(model_class) -> dict:
    """
    Discover and return all event listeners for a model class.

    Args:
        model_class: The model class to scan for event listeners

    Returns:
        dict: Mapping of event names to list of listener methods
    """
    events = {}

    # Scan all methods in the class hierarchy
    for cls in model_class.__mro__:
        for method_name in dir(cls):
            if method_name in cls.__dict__:
                method = cls.__dict__[method_name]
                if hasattr(method, "_is_model_event") and method._is_model_event:
                    event_name = method._event_name
                    if event_name not in events:
                        events[event_name] = []
                    events[event_name].append(method)

    return events
