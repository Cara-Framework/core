"""
Event Base for the Cara framework.

This module provides the base Event class for defining and dispatching events in the application.
"""

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

    def register_event(self, event_cls: Type[Event]) -> None:
        """
        Register an Event class so its name() is known.

        If two Event classes return the same name(), raise EventNameConflictException.
        """
        event_name = event_cls.name()
        with self._lock:
            if event_name in self._registered_events:
                existing = self._registered_events[event_name]
                if existing is not event_cls:
                    raise EventNameConflictException(
                        f"Event name conflict: '{event_name}' already registered by {existing}."
                    )
            self._registered_events[event_name] = event_cls

    def subscribe(self, event_name: str, listener: Listener) -> None:
        """
        Subscribe a Listener instance to a given event_name.

        If the event_name was not previously registered via register_event(), listeners can still be
        added; dispatch() will raise if no listeners found.
        """
        with self._lock:
            if event_name not in self._listeners:
                self._listeners[event_name] = []
            self._listeners[event_name].append(listener)

    def dispatch(self, event: Event) -> None:
        """
        Dispatch an Event instance to all subscribed listeners.

        Laravel-style: If a listener implements ShouldQueue, it gets queued.
        Otherwise, it gets executed immediately.

        If no listeners are registered for this event's name(), raise ListenerNotFoundException.
        """
        event_name = event.name()
        listeners = self._listeners.get(event_name, [])
        if not listeners:
            raise ListenerNotFoundException(
                f"No listeners registered for event '{event_name}'."
            )

        for listener in listeners:
            # Laravel-style queue check: If listener implements ShouldQueue, queue it
            if self._should_queue(listener):
                self._queue_listener(listener, event)
            else:
                # Execute immediately
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
            listener: The listener to queue
            event: The event that triggered the listener

        Returns:
            True if queued successfully, False otherwise
        """
        try:
            # Create a job to handle the listener
            from cara.events.jobs import HandleListenerJob

            job = HandleListenerJob(listener, event)

            # Dispatch to queue using facade
            Queue.dispatch(job)
            return True

        except Exception as e:
            # Log error using facade
            Log.error(f"Failed to queue listener: {e}")
            return False

    def listen(
        self,
        event_cls: Type[Event],
        listener_cls: Type[Listener],
    ) -> None:
        """
        Laravel-style helper: Listener for event_cls ⇒ listener_cls.
        1) Registers the event class, if not already registered.
        2) Subscribes a new instance of listener_cls() to the event's name().
        """
        # 1) Register the event class
        self.register_event(event_cls)
        # 2) Subscribe the listener instance
        listener_instance = listener_cls()
        self.subscribe(event_cls.name(), listener_instance)
