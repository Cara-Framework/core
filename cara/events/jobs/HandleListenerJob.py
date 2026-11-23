"""
Queue job for handling event listeners in background.

This job is automatically created when event listeners implement ShouldQueue.
"""

from __future__ import annotations

import inspect
from typing import Any

from cara.context import ExecutionContext
from cara.events.contracts import Listener
from cara.events.Event import Event as EventDispatcher
from cara.events.PayloadGate import event_payload_gaps
from cara.exceptions import (
    CaraException,
    InvalidArgumentException,
    ListenerNotFoundException,
)
from cara.queues import QueuePayload
from cara.queues.contracts import BaseJob


def _resolve_event_class(dispatcher: EventDispatcher, event_class_name: str) -> type[Any]:
    """The registered event class ``event_class_name`` names.

    The hop carries the FULLY QUALIFIED name, like the listener: a bare class
    name matched the first registered event with that name, so two events
    called ``OrderPlaced`` in different modules rebuilt whichever registered
    first. A bare name — a message queued before this change — is still
    accepted when exactly one registered class carries it, and refused when
    it is ambiguous.
    """
    registered: dict[str, type[Any]] = {}
    for cls in dispatcher._registered_events.values():
        registered.setdefault(_listener_class_path(cls), cls)
    found = registered.get(event_class_name)
    if found is not None:
        return found
    if "." not in event_class_name:
        bare = [cls for cls in registered.values() if cls.__name__ == event_class_name]
        if len(bare) == 1:
            return bare[0]
        if len(bare) > 1:
            raise InvalidArgumentException(
                f"Event class name {event_class_name!r} is ambiguous: "
                f"{', '.join(sorted(_listener_class_path(c) for c in bare))} are "
                "all registered. Requeue the hop with the fully qualified name."
            )
    raise InvalidArgumentException(
        f"Event class {event_class_name!r} is not registered on the dispatcher. "
        "Register it with dispatcher.register_event() before dispatching."
    )


def _listener_class_path(cls: type[Any]) -> str:
    """The name a queued listener hop carries: module plus qualified name."""
    return f"{cls.__module__}.{cls.__qualname__}"


def _resolve_listener_class(
    dispatcher: EventDispatcher, listener_class_name: str
) -> type[Any]:
    """The subscribed listener class ``listener_class_name`` names.

    The hop carries the FULLY QUALIFIED name: a bare class name matched the
    first subscribed listener with that name, so ``orders.NotifyListener``
    ran in place of ``billing.NotifyListener``. A bare name — a message
    queued before this change — is still accepted when exactly one
    subscribed class carries it, and refused when it is ambiguous rather
    than running whichever happened to subscribe first.
    """
    subscribed: dict[str, type[Any]] = {}
    for bucket in (dispatcher._listeners, dispatcher._wildcard_listeners):
        for listeners in bucket.values():
            for lst in listeners:
                cls = lst.__class__
                subscribed.setdefault(_listener_class_path(cls), cls)
    found = subscribed.get(listener_class_name)
    if found is not None:
        return found
    if "." not in listener_class_name:
        bare = [cls for cls in subscribed.values() if cls.__name__ == listener_class_name]
        if len(bare) == 1:
            return bare[0]
        if len(bare) > 1:
            raise ListenerNotFoundException(
                f"Listener class name {listener_class_name!r} is ambiguous: "
                f"{', '.join(sorted(_listener_class_path(c) for c in bare))} are "
                "all subscribed. Requeue the hop with the fully qualified name."
            )
    raise ListenerNotFoundException(
        f"No subscribed listener {listener_class_name!r} was found "
        "on the event dispatcher (subscribe the listener before queueing)."
    )


def _instantiate_event(event_cls: type[Any], data: dict[str, Any]) -> Any:
    """Re-hydrate ``event_cls`` from its serialized ``data`` dict.

    Prefers a ``from_dict`` classmethod when the event class declares
    one (lets a custom event opt into stricter parsing — type
    narrowing, default repopulation, etc.). Otherwise falls back to
    the public-attribute kwargs path: strip ``_``-prefixed keys
    (private bookkeeping like ``_dispatch_id`` shouldn't reach the
    ``__init__``) and call ``event_cls(**public)``.

    Post-construction, run ``event_payload_gaps`` — the same gate the
    in-process dispatcher applies, reading ``REQUIRED_FIELDS`` or a
    ``validate_payload`` override — and refuse to hand a malformed
    event to the listener. Pre-fix the queue path skipped this gate entirely
    — a payload that the dispatcher would have rejected at fire-time
    silently round-tripped through serialization and reached the
    listener with missing required fields. That's worse than the
    sync path because the in-process error surfaces at the dispatch
    site while the queue path surfaces at the listener's first deref
    (often hours later, in a worker log, with no stack trace from
    the originating fire). Re-raising as ``ValueError`` lets the
    queue runner's standard retry+DLQ path own the recovery.
    """
    from_dict = getattr(event_cls, "from_dict", None)
    if callable(from_dict):
        event = from_dict(data)
    else:
        public = {k: v for k, v in data.items() if not str(k).startswith("_")}
        try:
            event = event_cls(**public)
        except TypeError as e:
            raise TypeError(
                f"Could not reconstruct event {event_cls.__name__} from serialized data: {e}"
            ) from e

    try:
        missing = event_payload_gaps(event)
    except Exception as exc:
        # Validator raised — surface with the originating event
        # class name so the worker log points straight at the bug.
        # Use InvalidArgumentException (a ValueError subclass, same type
        # the ``missing``-fields branch below raises) so the queue
        # runner's standard ValueError-based retry+DLQ path owns the
        # recovery uniformly — not a bare CaraException that the runner
        # treats as an unexpected crash.
        raise InvalidArgumentException(
            f"validate_payload() on rehydrated {event_cls.__name__} "
            f"raised {exc.__class__.__name__}: {exc}",
        ) from exc
    if missing:
        raise InvalidArgumentException(
            f"Queued {event_cls.__name__} failed payload validation "
            f"(REQUIRED_FIELDS/validate_payload); "
            f"missing/invalid fields: {missing!r}. Refusing to invoke "
            f"listener with a malformed payload — same gate the sync "
            f"dispatcher applies at fire time.",
        )
    return event


class HandleListenerJob(BaseJob):
    """
    Job to handle event listeners in background.

    This is Laravel-style: when a listener implements ShouldQueue,
    the Event dispatcher automatically creates this job and dispatches it.
    """

    # Event-specific queue settings
    default_queue = "events"

    @staticmethod
    def listener_path(listener_cls: type[Any]) -> str:
        """The ``listener_class`` a hop for ``listener_cls`` must carry."""
        return _listener_class_path(listener_cls)

    @staticmethod
    def event_payload(event: Any) -> dict[str, Any]:
        """The event's own payload as plain data, or raise naming the field.

        ``to_dict()`` when the event declares one; otherwise its public
        attributes — ``_``-prefixed bookkeeping never reached the rebuilt
        event's constructor anyway, and a live object in it would only make
        the envelope refuse the whole hop with a less useful message.
        """
        to_dict = getattr(event, "to_dict", None)
        if callable(to_dict):
            data = to_dict()
        else:
            data = {k: v for k, v in vars(event).items() if not str(k).startswith("_")}
        if not isinstance(data, dict):
            raise InvalidArgumentException(
                f"{type(event).__name__}.to_dict() must return a dict to be queued."
            )
        return QueuePayload.require_plain(data, "event_data")

    def __init__(
        self,
        listener_class: str,
        event_data: dict[str, Any],
        event_class: str,
        **kwargs: Any,
    ) -> None:
        self.listener_class = listener_class
        self.event_data = dict(event_data) if event_data else {}
        self.event_class = event_class
        # ``payload`` is derived from the fields above. The worker rebuilds
        # this job with every constructor-chain parameter, BaseJob's
        # ``payload`` included, and forwarding it as well raised "got
        # multiple values for keyword argument 'payload'".
        kwargs.pop("payload", None)
        super().__init__(
            payload={
                "listener_type": listener_class,
                "event_class": event_class,
            },
            **kwargs,
        )

    async def handle(self):
        """Rehydrate listener + event and execute.

        ``handle`` MUST be async: ``SignedJsonJobSerializer`` rejects a
        sync ``handle`` at both serialize and deserialize, and
        ``queue:work`` repeats the gate consumer-side, so a sync handler
        made this job undispatchable on the AMQP rail — the only rail
        cara ships. ``Event._queue_listener`` swallowed the rejection and
        returned ``False`` to a caller that ignores the return, so every
        ``ShouldQueue`` listener silently never ran. Product listeners were
        found carrying docstrings explaining that they deliberately stay
        in-process because "a queued listener would silently drop" the work —
        that is this bug, worked around.

        A sync ``listener.handle`` is blocking, so it goes through
        ``ExecutionContext.run_in_thread`` instead of stalling the
        worker's event loop. The container ``call`` is itself sync, so the
        whole DI-resolve-and-invoke moves into the thread together — doing
        the resolve on the loop and only the body in a thread would split
        the contextvar snapshot across two contexts.
        """
        app = EventDispatcher._resolve_application()
        if app is not None:
            try:
                dispatcher = app.make("events")
            except Exception as e:
                raise CaraException(
                    "HandleListenerJob requires the 'events' dispatcher binding on the application."
                ) from e
            if not isinstance(dispatcher, EventDispatcher):
                raise TypeError(
                    f"Container key 'events' must be an Event dispatcher, got {type(dispatcher)!r}"
                )
        else:
            dispatcher = EventDispatcher()

        event_cls = _resolve_event_class(dispatcher, self.event_class)
        event = _instantiate_event(event_cls, self.event_data)

        listener_cls = _resolve_listener_class(dispatcher, self.listener_class)

        listener: Listener
        if app is not None and hasattr(app, "make"):
            try:
                listener = app.make(listener_cls)
            except Exception:
                listener = listener_cls()
        else:
            listener = listener_cls()

        handler = listener.handle
        is_async = inspect.iscoroutinefunction(handler)

        if app is not None and hasattr(app, "call"):
            if is_async:
                return await app.call(handler, event)
            return await ExecutionContext.run_in_thread(app.call, handler, event)
        if is_async:
            return await handler(event)
        return await ExecutionContext.run_in_thread(handler, event)
