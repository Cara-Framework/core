"""
Queue job for handling event listeners in background.

This job is automatically created when event listeners implement ShouldQueue.
"""

from __future__ import annotations

from typing import Any

from cara.events import Event as EventDispatcher
from cara.events.contracts import Listener
from cara.exceptions import CaraException, InvalidArgumentException, ListenerNotFoundException
from cara.queues.contracts import BaseJob


def _resolve_event_class(dispatcher: EventDispatcher, event_class_name: str) -> type[Any]:
    for cls in dispatcher._registered_events.values():
        if cls.__name__ == event_class_name:
            return cls
    raise InvalidArgumentException(
        f"Event class {event_class_name!r} is not registered on the dispatcher. "
        "Register it with dispatcher.register_event() before dispatching."
    )


def _resolve_listener_class(
    dispatcher: EventDispatcher, listener_class_name: str
) -> type[Any]:
    seen: set[int] = set()
    for bucket in (dispatcher._listeners, dispatcher._wildcard_listeners):
        for listeners in bucket.values():
            for lst in listeners:
                cls = lst.__class__
                if cls.__name__ == listener_class_name:
                    return cls
                seen.add(id(cls))
    raise ListenerNotFoundException(
        f"No subscribed listener with class name {listener_class_name!r} was found "
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

    Post-construction, if the event ships a ``validate_payload``
    method (the same hook the in-process dispatcher gates on at
    Event.py:318), call it and refuse to hand a malformed event to
    the listener. Pre-fix the queue path skipped this gate entirely
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

    validator = getattr(event, "validate_payload", None)
    if callable(validator):
        try:
            missing = validator()
        except Exception as exc:
            # Validator raised — surface with the originating event
            # class name so the worker log points straight at the bug.
            raise CaraException(
                f"validate_payload() on rehydrated {event_cls.__name__} "
                f"raised {exc.__class__.__name__}: {exc}",
            ) from exc
        if missing:
            raise InvalidArgumentException(
                f"Queued {event_cls.__name__} failed validate_payload(); "
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
        super().__init__(
            payload={
                "listener_type": listener_class,
                "event_class": event_class,
            },
            **kwargs,
        )

    def handle(self):
        """Rehydrate listener + event and execute."""
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

        if app is not None and hasattr(app, "call"):
            return app.call(listener.handle, event)
        return listener.handle(event)
