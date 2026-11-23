from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar

from cara.facades import Log

_logger = logging.getLogger("cara.eloquent.observers")

#: The model classes whose observers are muted in THIS execution context
#: (``save_quietly`` / ``delete_quietly``). A ``ContextVar``, the way the
#: connection registry isolates request state: the quiet saves used to flip
#: the CLASS flag ``__has_events__``, so while one thread or task saved
#: quietly every concurrent save of that model class lost its observer events.
#: Each thread starts from the empty default and each asyncio task from a copy
#: of its creator's context, so the mute reaches only the quiet call's own
#: work. An immutable tuple, reset by token, so no context shares a mutation.
_MUTED_CLASSES: ContextVar[tuple[type, ...]] = ContextVar(
    "cara_muted_model_events", default=()
)


class ObservesEvents:
    #: Off unless an instance turns its events off. Declared on the class so
    #: the four lifecycle checks of every hydrated row are a plain attribute
    #: read — without it each one fell through to the model's ``__getattr__``
    #: and its raised ``AttributeError``.
    _events_disabled: bool = False

    def observe_events(self, model, event):
        if getattr(model, "_events_disabled", False):
            return
        muted = _MUTED_CLASSES.get()
        # ``isinstance`` mirrors the class flag it replaces, which subclasses
        # inherited; it also covers the fresh instance ``save()`` creates a
        # new record through (``Model.create``), not just ``self``.
        if muted and isinstance(model, muted):
            return
        if model.__has_events__:
            for observer in model.__observers__.get(model.__class__, []):
                # An observer that does not handle this event is skipped
                # BEFORE the call: catching AttributeError around the call
                # also swallowed, silently and unlogged, every AttributeError
                # raised inside a real handler.
                handler = getattr(observer, event, None)
                if handler is None:
                    continue
                try:
                    handler(model)
                except Exception as exc:
                    try:
                        Log.error(
                            "Observer %s.%s failed: %s: %s",
                            observer.__class__.__name__,
                            event,
                            exc.__class__.__name__,
                            exc,
                            category="cara.eloquent.observers",
                            exc_info=True,
                        )
                    except Exception:
                        _logger.error("observer error handler failed", exc_info=True)

    @classmethod
    def observe(cls, observer):
        if cls in cls.__observers__:
            cls.__observers__[cls].append(observer)
        else:
            cls.__observers__.update({cls: [observer]})

    @classmethod
    @contextmanager
    def muted_events(cls):
        """Mute this class's observers for the enclosed work, in this context only."""
        token = _MUTED_CLASSES.set((*_MUTED_CLASSES.get(), cls))
        try:
            yield cls
        finally:
            _MUTED_CLASSES.reset(token)

    @classmethod
    def without_events(cls):
        """Sets __has_events__ attribute on model to false."""
        cls.__has_events__ = False
        return cls

    @classmethod
    def with_events(cls):
        """Sets __has_events__ attribute on model to True."""
        cls.__has_events__ = True
        return cls
