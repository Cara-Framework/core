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
from typing import Callable, Dict, List, Type, Optional

from cara.events.contracts import Event, Listener
from cara.exceptions import EventNameConflictException
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

    _app = None

    def __init__(self):
        # Mapping: event_name -> list of Listener instances
        self._listeners: Dict[str, List[Listener]] = {}
        # Wildcard listeners: pattern -> list of listeners
        self._wildcard_listeners: Dict[str, List[Listener]] = {}
        # Keep track of registered event names to avoid conflicts
        self._registered_events: Dict[str, Type[Event]] = {}
        self._lock = Lock()

    @classmethod
    def _resolve_application(cls):
        if cls._app is not None:
            return cls._app
        try:
            from bootstrap import application
            cls._app = application
            return application
        except Exception:
            return None

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

    def subscribe(self, event_name: str, listener: Optional[Listener] = None) -> None:
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

    def has_listeners(self, event_name: str) -> bool:
        """
        Check if there are any listeners for the given event name.

        Checks both direct listeners and wildcard listeners.

        Args:
            event_name: Event name to check for listeners

        Returns:
            True if there are listeners, False otherwise
        """
        if event_name in self._listeners and self._listeners[event_name]:
            return True
        if self._get_matching_wildcard_listeners(event_name):
            return True
        return False

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

        If event propagation is stopped by any listener, subsequent listeners are not called.

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
            return

        for listener in all_listeners:
            if hasattr(event, 'is_propagation_stopped') and event.is_propagation_stopped:
                break

            # Laravel-style queue check: If listener implements ShouldQueue, queue it
            if self._should_queue(listener):
                self._queue_listener(listener, event)
                continue

            # In-process listener: always await to completion.
            #
            # Rationale: ExecutionContext.is_sync() controls Bus job dispatch
            # (immediate vs queued) — it is NOT an event-dispatch flag. If a
            # listener should run out-of-band, it must implement ShouldQueue
            # (handled above). Firing in-process listeners as background tasks
            # here is unsafe: callers (e.g. queue workers) commonly drive
            # dispatch() via asyncio.run(), which closes the loop as soon as
            # the awaited coroutine returns and silently cancels any
            # detached tasks mid-execution — dropping downstream work
            # (e.g. cascading job dispatches) without error.
            #
            # Prometheus instrumentation — import lazily so the
            # events module stays usable in contexts that don't boot
            # the services application (e.g. cara tests). Records
            # invocation count + duration keyed on the listener's
            # class name; bounded cardinality regardless of event
            # volume.
            import time as _t
            try:
                from app.support.Metrics import Metrics as _M
            except Exception:
                _M = None  # type: ignore[assignment]

            _lst_name = listener.__class__.__name__
            _lst_start = _t.time()
            _lst_outcome = "success"
            _lst_propagate = bool(getattr(listener, "propagate_failures", False))
            try:
                app = self._resolve_application()
                if app is not None and hasattr(app, "call"):
                    result = app.call(listener.handle, event)
                    if inspect.isawaitable(result):
                        await result
                elif inspect.iscoroutinefunction(listener.handle):
                    await listener.handle(event)
                else:
                    listener.handle(event)
            except Exception as _listener_exc:
                _lst_outcome = "failure"
                try:
                    from cara.facades import Log
                    Log.error(
                        f"Event listener {_lst_name} failed: "
                        f"{_listener_exc.__class__.__name__}: {_listener_exc}",
                        category="cara.events",
                    )
                except Exception:
                    pass
                # Pipeline-critical listeners opt in via
                # ``propagate_failures = True``. Re-raising lets the
                # upstream job/queue treat the dispatch as failed and
                # retry instead of marking success and silently halting
                # the chain. Observability listeners (metrics, search
                # indexing, broadcasts) keep the legacy permissive
                # default so a flaky third-party can't take down the
                # whole pipeline. Metrics are recorded in ``finally``
                # below regardless of which branch we take, so the
                # raise is enough on its own here.
                if _lst_propagate:
                    raise
            finally:
                if _M is not None:
                    try:
                        _M.listener_invocations_total.labels(
                            listener=_lst_name, outcome=_lst_outcome,
                        ).inc()
                        _M.listener_duration_seconds.labels(
                            listener=_lst_name,
                        ).observe(_t.time() - _lst_start)
                    except (ImportError, AttributeError):
                        pass

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

    def _handle_task_exception(self, task: asyncio.Task) -> None:
        """
        Handle exceptions from fire-and-forget async listener tasks.

        Called when a fire-and-forget task completes. If the task raised an exception,
        logs it via Log.error() to prevent silent exception loss.

        Args:
            task: The completed asyncio task
        """
        try:
            task.result()
        except asyncio.CancelledError:
            pass  # Task was cancelled, ignore
        except Exception as e:
            Log.error(f"Fire-and-forget listener failed with exception: {str(e)}", exc_info=True)

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

    # Strong refs to fire-and-forget tasks. ``asyncio.create_task``
    # only weakly tracks tasks in the loop registry; without an extra
    # strong ref the GC can collect a Task whose owning code went
    # out of scope, and the coroutine simply vanishes mid-flight.
    # Tasks remove themselves from this set when done.
    _pending_tasks: "set[asyncio.Task]" = set()

    @classmethod
    def _track(cls, task: asyncio.Task) -> None:
        cls._pending_tasks.add(task)
        task.add_done_callback(cls._pending_tasks.discard)

    @staticmethod
    async def fire(event: Event) -> None:
        """
        Fire an event (alias for dispatch).

        This is an async method so callers can ``await Event.fire(evt)``.
        Internally dispatches through the listener pipeline.

        For sync contexts use :meth:`fire_sync` instead.

        Args:
            event: The event instance to fire
        """
        app = Event._resolve_application()
        if app is not None:
            instance = app.make("events")
        else:
            instance = Event()
        await instance.dispatch(event)

    @staticmethod
    def fire_sync(event: Event) -> None:
        """
        Synchronous variant of :meth:`fire` for use outside an async context.

        Handles both "no running loop" and "loop already running" edge cases.

        Args:
            event: The event instance to fire
        """
        from cara.context import ExecutionContext

        app = Event._resolve_application()
        if app is not None:
            instance = app.make("events")
        else:
            instance = Event()
        coro = instance.dispatch(event)

        if ExecutionContext.is_sync():
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                asyncio.run(coro)
            else:
                Event._track(asyncio.create_task(coro))
        else:
            Event._track(asyncio.create_task(coro))
