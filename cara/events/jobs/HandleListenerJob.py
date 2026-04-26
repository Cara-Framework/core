"""
Queue job for handling event listeners in background.

This job is automatically created when event listeners implement ShouldQueue.
"""

from typing import Any, Dict, Type

from cara.events import Event as EventDispatcher
from cara.events.contracts import Listener
from cara.queues.contracts import BaseJob


def _resolve_event_class(
    dispatcher: EventDispatcher, event_class_name: str
) -> Type[Any]:
    for cls in dispatcher._registered_events.values():
        if cls.__name__ == event_class_name:
            return cls
    raise ValueError(
        f"Event class {event_class_name!r} is not registered on the dispatcher. "
        "Register it with dispatcher.register_event() before dispatching."
    )


def _resolve_listener_class(
    dispatcher: EventDispatcher, listener_class_name: str
) -> Type[Any]:
    seen: set[int] = set()
    for bucket in (dispatcher._listeners, dispatcher._wildcard_listeners):
        for listeners in bucket.values():
            for lst in listeners:
                cls = lst.__class__
                if cls.__name__ == listener_class_name:
                    return cls
                seen.add(id(cls))
    raise ValueError(
        f"No subscribed listener with class name {listener_class_name!r} was found "
        "on the event dispatcher (subscribe the listener before queueing)."
    )


def _instantiate_event(event_cls: Type[Any], data: Dict[str, Any]) -> Any:
    from_dict = getattr(event_cls, "from_dict", None)
    if callable(from_dict):
        return from_dict(data)

    public = {k: v for k, v in data.items() if not str(k).startswith("_")}
    try:
        return event_cls(**public)
    except TypeError as e:
        raise TypeError(
            f"Could not reconstruct event {event_cls.__name__} from serialized data: {e}"
        ) from e


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
        event_data: Dict[str, Any],
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
                raise RuntimeError(
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
