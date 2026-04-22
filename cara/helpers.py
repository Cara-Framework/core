"""Re-exports commonly used helpers as a convenience module.

Importing from ``cara.helpers`` lets application code write::

    from cara.helpers import env, config, route, abort, abort_if, abort_unless, safe_call

without needing to know the canonical module path of each helper.
"""

from typing import Any, Callable, Dict, Optional, TypeVar

from cara.configuration import config
from cara.environment.Environment import env
from cara.exceptions.types.http import HttpException


def route(name: str, params: Optional[Dict[str, Any]] = None) -> str:
    """Generate the URL for a named route (Laravel ``route('users.show', {id:1})``).

    Resolves the application router through the container and delegates to
    ``Router.url()``. Raises ``RouteNotFoundException`` if the name is unknown.
    """
    from bootstrap import application
    router = application.make("router")
    return router.url(name, params)


def abort(status_code: int, message: Optional[str] = None, **extra: Any) -> None:
    """Immediately stop request handling and return an HTTP error response.

    Laravel-style ``abort(404)`` / ``abort(403, 'Forbidden')``. The framework's
    exception handler converts this into a JSON error response with the
    given status code.

    Extra kwargs are attached to the response body (e.g. ``abort(422,
    'Invalid', field='email')``).
    """
    default_messages = {
        400: "Bad request",
        401: "Unauthenticated",
        403: "Forbidden",
        404: "Not found",
        405: "Method not allowed",
        409: "Conflict",
        410: "Gone",
        422: "Unprocessable entity",
        429: "Too many requests",
        500: "Server error",
        503: "Service unavailable",
    }
    msg = message or default_messages.get(status_code, "Error")
    raise HttpException(msg, status_code=status_code, **extra)


def abort_if(condition: Any, status_code: int, message: Optional[str] = None, **extra: Any) -> None:
    """Abort with the given status code if ``condition`` is truthy."""
    if condition:
        abort(status_code, message, **extra)


def abort_unless(condition: Any, status_code: int, message: Optional[str] = None, **extra: Any) -> None:
    """Abort with the given status code unless ``condition`` is truthy."""
    if not condition:
        abort(status_code, message, **extra)


T = TypeVar("T")


def safe_call(
    fn: Callable[..., T],
    *args: Any,
    default: Optional[T] = None,
    log_message: Optional[str] = None,
    reraise: Optional[tuple] = None,
    **kwargs: Any,
) -> Optional[T]:
    """Run ``fn(*args, **kwargs)`` and swallow exceptions, returning ``default``.

    Wraps the ubiquitous::

        try:
            result = fn(...)
        except Exception as e:
            Log.warning(f"...: {e}")
            result = DEFAULT

    pattern in one call. ``log_message`` is formatted with ``{error}`` if
    provided (e.g. ``log_message='Failed to load user: {error}'``). Exception
    types listed in ``reraise`` are not swallowed.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as error:  # noqa: BLE001 — intentional broad catch
        if reraise and isinstance(error, reraise):
            raise
        if log_message is not None:
            try:
                from cara.facades import Log
                msg = log_message.format(error=error) if "{error}" in log_message else f"{log_message}: {error}"
                Log.warning(msg)
            except Exception:
                pass
        return default


__all__ = [
    "env",
    "config",
    "route",
    "abort",
    "abort_if",
    "abort_unless",
    "safe_call",
]
