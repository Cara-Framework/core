"""The exception classes that mean "the cache backend failed to answer"."""

from __future__ import annotations


def cache_backend_fault_types() -> tuple[type[BaseException], ...]:
    """Exception classes a cache call raises when the BACKEND itself failed.

    redis-py raises its OWN ``ConnectionError`` / ``TimeoutError`` /
    ``ResponseError``, all rooted at ``redis.exceptions.RedisError`` — they
    are NOT the builtins of the same name. A handler that names the builtins
    never runs for a real Redis fault: the error escapes it as an unhandled
    job failure while the degradation path it declared stays dead code.
    ``OSError`` covers the builtin socket/file faults (the builtin
    ``ConnectionError`` / ``TimeoutError`` are its subclasses) that the file
    driver and a raw socket raise.

    This is the one place that knows the mapping, so every caller that
    degrades on a backend fault catches the same set — a codec or
    configuration error is NOT in it and keeps propagating.

    Resolved at call time because redis-py is an optional dependency of the
    framework (``RedisCacheDriver`` is the only module that requires it), and
    an install without it must still be able to import the callers.
    """
    try:
        from redis.exceptions import RedisError  # local: heavy optional dep

        return (RedisError, OSError)
    except ImportError:
        return (OSError,)
