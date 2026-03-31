"""
Event Base for the Cara framework.

This module provides the base Event class for defining and dispatching events in the application.

Supports:
- Direct event listener subscription
- Event subscriber classes (for grouping related listeners)
- Wildcard event listeners (matching event name patterns)
- Queued event listeners (via ShouldQueue interface)
- Async/sync listener execution
"""

import asyncio
import inspect
from threading import Lock
from typing import Callable, Dict, List, Type

from cara.events.contracts import Event, Listener
from cara.exceptions import EventNameConflictException, ListenerNotFoundException
from cara.facades import Log, Queue
from cara.queues.contracts import ShouldQueue


class EventSubscriber:
    """
    Base class for event subscribers.

    Group related event listeners together by extending this class
    and implementing the subscribe() method.

    Example:
        class UserEventSubscriber(EventSubscriber):
            def subscribe(self, dispatcher):
                dispatcher.listen('user.created', self.on_user_created)
                dispatcher.listen('user.updated', self.on_user_updated)

            def on_user_created(self, event):
                # Handle user created
                pass

            def on_user_updated(self, event):
                # Handle user updated
                pass
    """

    def subscribe(self, dispatcher: "Event") -> None:
        """
        Subscribe to events in the dispatcher.

        Args:
            dispatcher: The Event dispatcher instance
        """
        raise NotImplementedError("Subscriber must implement subscribe() method")


class Event:
    """
    Central event dispatcher.

    Features:
    - Subscribe Listener classes to event names
    - Support for event subscriber classes (group related listeners)
    - Wildcard event listeners (*.user, user.*, etc)
    - Laravel-style: Listeners implementing ShouldQueue get queued
    - Async/sync listener support
    """

    def __init__(self):
        # Mapping: event_name -> list of Listener instances
        self._listeners: Dict[str, List[Listener]] = {}
        # Wildcard listeners: pattern -> list of listeners
        self._wildcard_listeners: Dict[str, List[Listener]] = {}
        # Keep track of registered event names to avoid conflicts
        self._registered_events: Dict[str, Type[Event]] = {}
        self._lock = Lock()

    def register_event(self, event_class: Type[Event]) -> None:
        """
        Register an Event class by its name() method.

        Args:
            event_class: The Event class to register

        Raises:
            EventNameConflictException: If another event class has the same name
        """
        # Get event name - support both property and method
        if hasattr(event_class, "name"):
            if isinstance(event_class.name, str):
                event_name = event_class.name
            else:
                event_name = event_class.name()
        else:
            event_name = event_class.__name__.lower()

        if event_name in self._registered_events:
            existing_class = self._registered_events[event_name]
            if existing_class != event_class:
                raise EventNameConflictException(
                    f"Event name '{event_name}' is already registered by {existing_class.__name__}."
                )

        self._registered_events[event_name] = event_class

    def subscribe(self, event_name: str, listener: Listener = None) -> None:
        """
        Subscribe a Listener instance or EventSubscriber to events.

        Supports three patterns:
        1. Direct listener: subscribe(event_name, listener_instance)
        2. Event subscriber class: subscribe(MyEventSubscriber)
        3. Wildcard listeners: subscribe('user.*', listener) or subscribe('*.created', listener)

        Args:
            event_name: Event name, wildcard pattern, or EventSubscriber class
            listener: The Listener instance (optional if event_name is EventSubscriber class)

        Example:
            # Direct listener
            dispatcher.subscribe('user.created', my_listener)

            # Wildcard listener
            dispatcher.subscribe('user.*', my_listener)
            dispatcher.subscribe('*.created', my_listener)

            # Event subscriber class
            dispatcher.subscribe(UserEventSubscriber)
        """
        # Handle EventSubscriber classes
        if listener is None and isinstance(event_name, type):
            # event_name is actually an EventSubscriber class
            if issubclass(event_name, EventSubscriber):
                subscriber_instance = event_name()
                subscriber_instance.subscribe(self)
                return

        # Handle wildcard patterns
        if "*" in event_name:
            with self._lock:
                if event_name not in self._wildcard_listeners:
                    self._wildcard_listeners[event_name] = []
                self._wildcard_listeners[event_name].append(listener)
            return

        # Handle direct event listener subscription
        with self._lock:
            if event_name not in self._listeners:
                self._listeners[event_name] = []
            self._listeners[event_name].append(listener)

    def listen(self, event_name: str, callback: Callable) -> None:
        """
        Alias for subscribe that accepts a callback function.

        Useful for registering simple callback handlers without creating Listener classes.

        Example:
            dispatcher.listen('user.created', lambda event: print(f"User created: {event.user}"))

        Args:
            event_name: Event name to listen for
            callback: Function to call when event is dispatched
        """
        # Create a simple listener wrapper for the callback
        class CallbackListener(Listener):
            def handle(self, event):
                return callback(event)

        listener = CallbackListener()
        self.subscribe(event_name, listener)

    async def dispatch(self, event: Event) -> None:
        """
        Dispatch an Event instance to all subscribed listeners.

        Handles:
        - Direct event listener subscription
        - Wildcard event patterns (user.*, *.created, etc)
        - Queued listeners (via ShouldQueue interface)
        - Async/sync listener execution

        Laravel-style: If a listener implements ShouldQueue, it gets queued.
        Otherwise, it gets executed immediately.

        Raises ListenerNotFoundException if no listeners match the event.

        Note: This is async to properly handle async listeners in sync mode.
        """
        # Get event name - support both property and method
        if hasattr(event, "name"):
            event_name = event.name if isinstance(event.name, str) else event.name()
        else:
            event_name = event.__class__.__name__.lower()

        # Get direct listeners for this event name
        direct_listeners = self._listeners.get(event_name, []).copy()

        # Get wildcard listeners that match this event name
        wildcard_listeners = self._get_matching_wildcard_listeners(event_name)

        # Combine all listeners
        all_listeners = direct_listeners + wildcard_listeners

        if not all_listeners:
            raise ListenerNotFoundException(
                f"No listeners registered for event '{event_name}'."
            )

        # Check if we're in sync mode
        from cara.context import ExecutionContext

        is_sync = ExecutionContext.is_sync()

        for listener in all_listeners:
            # Laravel-style queue check: If listener implements ShouldQueue, queue it
            if self._should_queue(listener):
                self._queue_listener(listener, event)
            else:
                # Execute immediately - handle both sync and async listeners
                if inspect.iscoroutinefunction(listener.handle):
                    # Async listener
                    if is_sync:
                        # Sync mode - await for completion (blocking)
                        await listener.handle(event)
                    else:
                        # Async mode - fire and forget (non-blocking)
                        asyncio.create_task(listener.handle(event))
                else:
                    # Sync listener - call directly
                    listener.handle(event)

    def _get_matching_wildcard_listeners(self, event_name: str) -> List[Listener]:
        """
        Find all wildcard listeners that match the given event name.

        Supports patterns like:
        - "user.*" matches "user.created", "user.updated", etc
        - "*.created" matches "user.created", "post.created", etc
        - "*" matches all events

        Args:
            event_name: The event name to match

        Returns:
            List of listeners matching the wildcard patterns
        """
        matching_listeners = []

        for pattern, listeners in self._wildcard_listeners.items():
            if self._matches_wildcard(event_name, pattern):
                matching_listeners.extend(listeners)

        return matching_listeners

    def _matches_wildcard(self, event_name: str, pattern: str) -> bool:
        """
        Check if an event name matches a wildcard pattern.

        Args:
            event_name: The event name to check
            pattern: The wildcard pattern (e.g., "user.*", "*.created")

        Returns:
            True if event_name matches the pattern, False otherwise
        """
        import fnmatch
        return fnmatch.fnmatch(event_name, pattern)

    def _should_queue(self, listener: Listener) -> bool:
        """
        Check if listener should be queued (Laravel-style ShouldQueue interface).

        Args:
            listener: The listener instance

        Returns:
            True if listener implements ShouldQueue, False otherwise
        """
        return isinstance(listener, ShouldQueue)

    def _queue_listener(self, listener: Listener, event: Event) -> bool:
        """
        Queue a listener for background processing.

        Args:
            listener: The listener instance to queue
            event: The event instance

        Returns:
            True if successfully queued, False otherwise
        """
        # Queue the listener using the queue facade
        queue_name = getattr(listener, "queue", "default")
        routing_key = getattr(
            listener, "routing_key", f"listener.{event.__class__.__name__.lower()}"
        )

        try:
            # Create a HandleListenerJob
            from cara.events.jobs import HandleListenerJob

            job = HandleListenerJob(
                listener_class=listener.__class__.__name__,
                event_data=event.to_dict()
                if hasattr(event, "to_dict")
                else event.__dict__,
                event_class=event.__class__.__name__,
            )

            # Dispatch to queue
            Queue.withQueue(queue_name).withRoutingKey(routing_key).dispatch(job)
            return True

        except Exception as e:
            Log.error(f"Failed to queue listener: {str(e)}")
            return False

    @staticmethod
    def fire(event: Event) -> None:
        """
        Static method to fire an event (alias for dispatch).

        Args:
            event: The event instance to fire
        """
        instance = Event()
        instance.dispatch(event)
