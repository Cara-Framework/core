"""
Event Base for the Cara framework.

This module provides the base Event class for defining and dispatching events in the application.
"""

import asyncio
import inspect
from threading import Lock
from typing import Dict, List, Type

from cara.events.contracts import Event, Listener
from cara.exceptions import EventNameConflictException, ListenerNotFoundException
from cara.facades import Log, Queue
from cara.queues.contracts import ShouldQueue


class Event:
    """
    Central event dispatcher.

    - You subscribe Listener classes to an event name.
    - When you dispatch an Event instance, all subscribed listeners' handle() will run.
    - Laravel-style: If listeners implement ShouldQueue, they get queued instead of executed immediately.
    """

    def __init__(self):
        # Mapping: event_name -> list of Listener instances
        self._listeners: Dict[str, List[Listener]] = {}
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

    def subscribe(self, event_name: str, listener: Listener) -> None:
        """
        Subscribe a Listener instance to an event name.

        Args:
            event_name: Name of the event to listen for
            listener: The Listener instance
        """
        with self._lock:
            if event_name not in self._listeners:
                self._listeners[event_name] = []
            self._listeners[event_name].append(listener)

    async def dispatch(self, event: Event) -> None:
        """
        Dispatch an Event instance to all subscribed listeners.

        Laravel-style: If a listener implements ShouldQueue, it gets queued.
        Otherwise, it gets executed immediately.

        If no listeners are registered for this event's name(), raise ListenerNotFoundException.

        Note: This is async to properly handle async listeners in sync mode.
        """
        # Get event name - support both property and method
        if hasattr(event, "name"):
            event_name = event.name if isinstance(event.name, str) else event.name()
        else:
            event_name = event.__class__.__name__.lower()

        listeners = self._listeners.get(event_name, [])
        if not listeners:
            raise ListenerNotFoundException(
                f"No listeners registered for event '{event_name}'."
            )

        # Check if we're in sync mode
        from cara.context import ExecutionContext

        is_sync = ExecutionContext.is_sync()

        for listener in listeners:
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
