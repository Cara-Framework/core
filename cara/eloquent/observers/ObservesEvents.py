from contextlib import contextmanager


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
                            f"Observer {observer.__class__.__name__}.{event} failed: "
                            f"{exc.__class__.__name__}: {exc}",
                            category="cara.eloquent.observers",
                        )
                    except Exception:
                        pass

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

    def save_quietly(self):
        """Save without firing events (instance-level, no race condition)."""
        self._events_disabled = True
        try:
            return self.save()
        finally:
            self._events_disabled = False

    def delete_quietly(self):
        """Delete without firing events (instance-level, no race condition)."""
        self._events_disabled = True
        try:
            return self.delete()
        finally:
            self._events_disabled = False

    @classmethod
    @contextmanager
    def without_events_context(cls):
        """Context manager for disabling events safely (restores state after)."""
        previous = cls.__has_events__
        cls.__has_events__ = False
        try:
            yield cls
        finally:
            cls.__has_events__ = previous
