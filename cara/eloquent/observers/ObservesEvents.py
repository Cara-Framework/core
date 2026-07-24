from __future__ import annotations

import logging

_logger = logging.getLogger("cara.eloquent.observers")


class ObservesEvents:
    def observe_events(self, model, event):
        if getattr(model, "_events_disabled", False):
            return
        if model.__has_events__:
            for observer in model.__observers__.get(model.__class__, []):
                try:
                    getattr(observer, event)(model)
                except AttributeError:
                    pass
                except Exception as exc:
                    try:
                        from cara.facades import Log

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
