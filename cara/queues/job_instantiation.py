"""Shared helpers for queue job instantiation and container binding."""

import inspect


def instantiate_job(application, raw, init_args=()):
    """Instantiate a job from a class or return the instance, stamping the container."""
    if inspect.isclass(raw):
        if hasattr(application, "make") and not init_args:
            try:
                instance = application.make(raw)
            except Exception:
                instance = raw(*init_args)
        else:
            instance = raw(*init_args)
    else:
        instance = raw

    if hasattr(instance, "__dict__"):
        instance._app = application
        if hasattr(type(instance), "_app"):
            type(instance)._app = application

    return instance
