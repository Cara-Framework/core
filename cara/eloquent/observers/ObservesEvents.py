from __future__ import annotations

import logging

from cara.facades import Log

_logger = logging.getLogger("cara.eloquent.observers")


class ObservesEvents:
    def observe_events(self, model, event):
        if getattr(model, "_events_disabled", False):
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
    def without_events(cls):
        """Sets __has_events__ attribute on model to false."""
        cls.__has_events__ = False
        return cls

    @classmethod
    def with_events(cls):
        """Sets __has_events__ attribute on model to True."""
        cls.__has_events__ = True
        return cls
