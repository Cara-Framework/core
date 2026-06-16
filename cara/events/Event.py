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

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from contextvars import ContextVar
from threading import RLock

from cara.events.contracts import Event, Listener
from cara.exceptions import EventDispatchCycleException, EventNameConflictException
from cara.facades import Log
from cara.queues.contracts import ShouldQueue

# Per-task stack of event names currently being dispatched. Used to
# detect re-entrant dispatch of an event whose handler is still on
# the stack, which would otherwise recurse until the Python stack
# overflows. ContextVar (not threadlocal) so the chain follows the
# asyncio task that drives dispatch, including nested ``await``s.
_dispatch_stack: ContextVar[tuple[str, ...]] = ContextVar(
    "cara_event_dispatch_stack", default=()
)


def fresh_dispatch_scope():
    """Context manager that clears the in-flight event-dispatch stack
    for the duration of the ``with`` block, then restores the prior
    stack on exit.

    Use at natural job boundaries (e.g. ``Bus._run_sync_with_tracking``)
    so a sync-dispatched child job doesn't inherit its caller's
    listener-context stack. Without this, a listener that dispatches
    a child job whose own ``handle()`` fires the same event type (for
    a different entity) trips the cycle guard — even though it's a
    legitimate fan-out tree, not a recursive loop. Queued mode doesn't
    need this because each worker has its own contextvar context;
    sync mode reuses the caller's context and that's where the leak
    happens.

    Cycle protection is preserved WITHIN the wrapped block — the
    fresh stack starts empty but accumulates as the inner code
    dispatches its own events, so a genuine self-recursive listener
    inside the job still raises.
    """
    import contextlib

    @contextlib.contextmanager
    def _scope():
        token = _dispatch_stack.set(())
        try:
            yield
        finally:
            _dispatch_stack.reset(token)

    return _scope()


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

    def subscribe(self, dispatcher: Event) -> None:
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

    def meta(self, key: str, default=None):
        """Read a value from the event's metadata dict.

        Consolidates 5 different null-guard patterns across listeners::

            # Before: (event.metadata or {}).get("priority", "default")
            # After:  event.meta("priority", "default")
        """
        md = getattr(self, "metadata", None) or {}
        return md.get(key, default)

    def __init__(self):
        # Mapping: event_name -> list of Listener instances
        self._listeners: dict[str, list[Listener]] = {}
        # Wildcard listeners: pattern -> list of listeners
        self._wildcard_listeners: dict[str, list[Listener]] = {}
        # Keep track of registered event names to avoid conflicts
        self._registered_events: dict[str, type[Event]] = {}
        # Per-topic count of dispatches that fired without any
        # subscribed listener. ``dispatch`` increments here when
        # ``has_listeners`` is False at fire time. Operators wrap a
        # Prometheus counter on top of ``orphan_dispatch_count`` so
        # a regression (renamed listener, typo'd subscribe topic)
        # surfaces in dashboards rather than vanishing into the
        # dispatcher's silent no-op branch.
        self._orphan_dispatch_counts: dict[str, int] = {}
        # Reentrant: ``dispatch`` snapshots listeners while holding the
        # lock and then calls ``_get_matching_wildcard_listeners``,
        # which also acquires for its own iteration. A plain ``Lock``
        # would deadlock the second acquire on the same thread.
        self._lock = RLock()

    def orphan_dispatch_count(self, event_name: str) -> int:
        """Return how many times ``event_name`` was dispatched with
        zero registered listeners since this dispatcher booted.

        Tests and operator dashboards use this to detect events that
        leak into the void — a common bug shape when a listener is
        renamed but the ``subscribe()`` call still references the old
        topic.
        """
        with self._lock:
            return self._orphan_dispatch_counts.get(event_name, 0)

    @classmethod
    def _resolve_application(cls):
        if cls._app is not None:
            return cls._app
        try:
            from bootstrap import application

            cls._app = application
            return application
        except (ImportError, RuntimeError, AttributeError):
            return None

    def register_event(self, event_class: type[Event]) -> None:
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

    def subscribe(self, event_name: str, listener: Listener | None = None) -> None:
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
                bucket = self._wildcard_listeners.setdefault(event_name, [])
                # Identity-dedup: subscribing the SAME listener instance
                # twice to the same pattern would otherwise invoke the
                # handler twice per fire — every double-subscribe is a
                # silent doubling of notifications / DB writes / job
                # dispatches. Comparison is by ``is`` (not ``==``) so a
                # listener class that overrides ``__eq__`` doesn't
                # accidentally collapse two distinct instances.
                if not any(existing is listener for existing in bucket):
                    bucket.append(listener)
            return

        # Handle direct event listener subscription
        with self._lock:
            bucket = self._listeners.setdefault(event_name, [])
            # Identity-dedup mirror of the wildcard path above. The
            # ``EventSubscriber.subscribe()`` flow re-runs on every app
            # boot AND from test setUp/tearDown — without this guard a
            # test that boots the app twice (TestCase per-method
            # ``setUp``) would silently double the listener list on the
            # second boot and every assertion on listener-invocation
            # counts would fail intermittently.
            if not any(existing is listener for existing in bucket):
                bucket.append(listener)

    def unsubscribe(self, event_name: str, listener: Listener) -> bool:
        """Remove ``listener`` from the bucket for ``event_name``.

        Returns ``True`` when a registration was removed, ``False``
        when no matching subscription existed (no-op, no raise).

        Why this exists
        ~~~~~~~~~~~~~~~
        The dispatcher was append-only — tests setting up per-case
        listeners had to either work around the leftover registrations
        OR teardown-recreate the dispatcher. Both shapes are noisy.
        More importantly: request-scoped listeners (a controller
        attaches a one-shot listener for the duration of a request)
        could not detach themselves at request end, leaking into the
        next request's dispatch and accumulating over the app's
        lifetime. Comparison is by ``is`` (identity), mirroring the
        dedup in :meth:`subscribe` so the same listener instance
        roundtrips cleanly.

        Args:
            event_name: Event name OR wildcard pattern the listener
                was subscribed to. Pattern must match what was passed
                to ``subscribe`` exactly — this method does NOT walk
                wildcards looking for the listener in a different
                bucket.
            listener: The listener instance to remove. Identity
                comparison (``is``), not equality.
        """
        bucket_map = self._wildcard_listeners if "*" in event_name else self._listeners
        with self._lock:
            bucket = bucket_map.get(event_name)
            if not bucket:
                return False
            for idx, existing in enumerate(bucket):
                if existing is listener:
                    bucket.pop(idx)
                    # Drop the empty bucket entry so ``has_listeners``
                    # returns False cleanly. Without this the bucket
                    # exists as ``[]`` and the next subscriber may
                    # mistake an empty list for a populated one in
                    # debug logging.
                    if not bucket:
                        bucket_map.pop(event_name, None)
                    return True
            return False

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
        with self._lock:
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

        # Per-event payload validation. Events opt in by declaring
        # ``REQUIRED_FIELDS`` (or implementing
        # :meth:`validate_payload`); a non-empty result means the
        # event is malformed (caller forgot a positional, passed
        # ``None`` for a typed field, etc.). We log + skip dispatch
        # rather than raise — the upstream job has already done its
        # work and may have queued the event for observability only,
        # so a fire-time raise would back-propagate a failure that
        # wasn't really there. The warning surfaces the bad payload
        # exactly once.
        validator = getattr(event, "validate_payload", None)
        if callable(validator):
            try:
                missing = validator()
            except Exception as _vexc:
                missing = None
                Log.warning("Event %s validate_payload() raised %s: %s; dispatching anyway", event_name, _vexc.__class__.__name__, _vexc, category='cara.events')
            if missing:
                Log.warning("Event %s failed validate_payload(); missing/invalid fields: %s. Skipping dispatch.", event_name, missing, category='cara.events')
                return

        # Cycle guard. If this same event name is already in flight on
        # the current task, a listener somewhere re-dispatched it —
        # left alone, this recurses until the Python stack overflows.
        # We raise instead so the responsible listener fails loudly
        # rather than crashing the whole process.
        stack = _dispatch_stack.get()
        if event_name in stack:
            chain = " -> ".join((*stack, event_name))
            raise EventDispatchCycleException(
                f"Event dispatch cycle detected for '{event_name}'. Chain: {chain}"
            )

        # Snapshot direct + wildcard listeners under the lock so a
        # concurrent ``subscribe()`` from another thread cannot mutate
        # the underlying lists while we iterate. Without the lock,
        # ``_get_matching_wildcard_listeners`` walks
        # ``_wildcard_listeners.items()`` directly and can raise
        # ``RuntimeError: dictionary changed size during iteration``.
        with self._lock:
            direct_listeners = list(self._listeners.get(event_name, []))
            wildcard_listeners = self._get_matching_wildcard_listeners(event_name)

        # Dedup by listener identity so a listener subscribed to BOTH
        # a specific event name AND a wildcard that matches it fires
        # exactly once. Insertion order is preserved (direct first,
        # then wildcards) so existing ordering guarantees hold.
        seen: set[int] = set()
        all_listeners: list[Listener] = []
        for _lst in (*direct_listeners, *wildcard_listeners):
            _ident = id(_lst)
            if _ident in seen:
                continue
            seen.add(_ident)
            all_listeners.append(_lst)

        if not all_listeners:
            # Surface the orphan dispatch via a per-topic counter so
            # operators can wire a Prometheus / log alert. The silent
            # no-op pre-fix made renamed listeners and typo'd topics
            # invisible — by the time the regression was noticed the
            # upstream job had already marked itself "succeeded" and
            # the orphaned event was unrecoverable.
            with self._lock:
                self._orphan_dispatch_counts[event_name] = (
                    self._orphan_dispatch_counts.get(event_name, 0) + 1
                )
            return

        token = _dispatch_stack.set((*stack, event_name))
        try:
            await self._invoke_listeners(event, all_listeners)
        finally:
            _dispatch_stack.reset(token)

    async def _invoke_listeners(
        self, event: Event, all_listeners: list[Listener]
    ) -> None:
        for listener in all_listeners:
            if hasattr(event, "is_propagation_stopped") and event.is_propagation_stopped:
                break

            # Laravel-style queue check: If listener implements ShouldQueue, queue it.
            # EXCEPT under ExecutionContext.sync() (--sync CLI / tests), where the
            # whole pipeline MUST run inline to completion. A queued listener in
            # sync mode pushes work onto a broker that no worker is draining (the
            # CLI exits right after), so the event-driven pipeline silently stalls
            # mid-flight — the "first product fully processed, the rest stranded
            # in the queue" divergence. In sync mode we fall through to the
            # in-process await path so the listener (and everything it dispatches)
            # runs inline. Sync = fully inline, identical outcome to async.
            from cara.context import ExecutionContext

            if self._should_queue(listener) and not ExecutionContext.is_sync():
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
            except (ImportError, RuntimeError):
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

                    Log.error("Event listener %s failed: %s: %s", _lst_name, _listener_exc.__class__.__name__, _listener_exc, category='cara.events')
                except (ImportError, RuntimeError):
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
                            listener=_lst_name,
                            outcome=_lst_outcome,
                        ).inc()
                        _M.listener_duration_seconds.labels(
                            listener=_lst_name,
                        ).observe(_t.time() - _lst_start)
                    except (ImportError, AttributeError):
                        pass

    def _get_matching_wildcard_listeners(self, event_name: str) -> list[Listener]:
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
        matching_listeners: list[Listener] = []

        with self._lock:
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
            job.queue = queue_name

            from cara.queues.contracts.Queueable import PendingDispatch

            pending = PendingDispatch(job)
            pending.with_routing_key(routing_key)
            pending._dispatch_now()
            return True

        except Exception as e:
            Log.error("Failed to queue listener: %s", str(e))
            # Pipeline-critical listeners opt into propagation via
            # ``propagate_failures = True``. Pre-fix this branch
            # swallowed every queue-side failure (broker offline,
            # serialisation error, missing routing key) and returned
            # ``False`` — the caller (``_invoke_listeners``) ignored
            # the return value, so the upstream job marked itself
            # successful while the listener never ran. Re-raising
            # here honours the same contract as the in-process
            # listener path and lets the queue worker retry.
            if getattr(listener, "propagate_failures", False):
                raise
            return False

    # Strong refs to fire-and-forget tasks. ``asyncio.create_task``
    # only weakly tracks tasks in the loop registry; without an extra
    # strong ref the GC can collect a Task whose owning code went
    # out of scope, and the coroutine simply vanishes mid-flight.
    # Tasks remove themselves from this set when done.
    _pending_tasks: set[asyncio.Task] = set()

    @classmethod
    def _track(cls, task: asyncio.Task) -> None:
        cls._pending_tasks.add(task)
        task.add_done_callback(cls._pending_tasks.discard)
        # Surface exceptions from fire-and-forget tasks via Log.error.
        # Without this callback, ``task.result()`` is never called and
        # any exception raised by the listener is silently dropped —
        # the worst failure mode for a bug-finding pipeline. The
        # ``_handle_task_exception`` helper existed for exactly this
        # purpose but was never wired up.
        task.add_done_callback(cls._handle_task_exception_static)

    @staticmethod
    def _handle_task_exception_static(task: asyncio.Task) -> None:
        """``add_done_callback`` adapter for :meth:`_handle_task_exception`.

        The instance method takes ``self`` plus the task; this
        wrapper drops the implicit ``self`` so the callback can be
        registered without a bound instance.
        """
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as e:
            try:
                from cara.facades import Log as _Log

                _Log.error(
                    "Fire-and-forget listener failed with exception: %s: %s",
                    e.__class__.__name__,
                    e,
                    category="cara.events",
                )
            except Exception:
                # Log facade may not be wired in a bare framework
                # context (cara unit tests). Re-raise to ``stderr``
                # as a last resort so the exception isn't fully
                # swallowed.
                import sys

                print(
                    f"[cara.events] fire-and-forget task raised "
                    f"{e.__class__.__name__}: {e}",
                    file=sys.stderr,
                )

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
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                asyncio.run(coro)
            else:
                Event._track(asyncio.create_task(coro))
