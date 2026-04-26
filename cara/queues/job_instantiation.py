"""Shared helpers for queue job instantiation and container binding."""

import inspect


def instantiate_job(application, raw, init_args=()):
    """Instantiate a job from a class or return the instance, stamping the container.

    The container path is preferred — it lets jobs declare type-hinted
    constructor dependencies and have them auto-resolved. We fall back
    to a raw constructor call when:

    1. The container resolution itself raises (e.g. an unbound contract
       in a half-booted test). In that case we LOG the failure before
       falling back so a misconfigured binding doesn't silently degrade
       to "ran with no DI" — the operator needs to know.
    2. The caller already has explicit ``init_args``: we must respect
       those rather than letting the container choose collaborators.
    3. The container doesn't expose a ``make`` method (rare, but
       happens during early boot or in skeletal test rigs).
    """
    if inspect.isclass(raw):
        if hasattr(application, "make") and not init_args:
            try:
                instance = application.make(raw)
            except Exception as e:
                # Surface the resolution failure once instead of letting
                # the silent fallback hide a genuine binding bug. We
                # avoid importing the Log facade unconditionally to keep
                # this module usable during early bootstrap; resolve it
                # lazily and degrade to ``print`` if even that fails.
                _emit_make_failure(raw, e)
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


def _emit_make_failure(job_class, error: Exception) -> None:
    """Log a container-resolution failure with stderr fallback."""
    try:
        from cara.facades import Log
        Log.warning(
            f"queues.instantiate_job: container.make({job_class.__name__}) "
            f"failed ({error.__class__.__name__}: {error}); falling back "
            f"to no-arg constructor",
            category="queues",
        )
    except Exception:
        # Log facade itself blew up — happens only during partial-boot.
        # Last resort: stderr so the failure isn't completely silent.
        import sys
        print(
            f"[queues.instantiate_job] container.make({job_class.__name__}) "
            f"failed: {error.__class__.__name__}: {error}",
            file=sys.stderr,
        )
