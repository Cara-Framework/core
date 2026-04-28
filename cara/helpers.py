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
            except Exception as e:
                from cara.facades import Log
                Log.warning(f"safe_call swallowed: {e}")
                return None
        return default


def tap(value: T, callback: Optional[Callable[[T], Any]] = None) -> T:
    """Pass ``value`` through ``callback`` then return ``value``.

    Mirrors Laravel's global ``tap()`` — useful for fluent chains
    that need to peek / mutate without breaking the chain::

        product = tap(repo.find(id), lambda p: p.touch())

    With no callback, returns the value unchanged (rare; use the
    one-arg form to keep grep-ability).
    """
    if callback is not None:
        callback(value)
    return value


def value(target: Any, *args: Any, **kwargs: Any) -> Any:
    """Return ``target()`` if callable, otherwise return ``target``.

    Mirrors Laravel's ``value()`` helper — lets APIs accept either a
    static value or a thunk in the same parameter slot::

        ttl = value(config_ttl)        # int   → int
        ttl = value(lambda: 30)        # thunk → 30
    """
    if callable(target):
        return target(*args, **kwargs)
    return target


def data_get(target: Any, key: Optional[str], default: Any = None) -> Any:
    """Read ``key`` from a nested dict / list using dot-notation.

    Thin alias around :meth:`cara.support.Arr.get` so the canonical
    Laravel helper name resolves at the global ``cara.helpers`` import
    path. Supports the ``"*"`` wildcard segment to map across lists.
    """
    from cara.support.Arr import Arr

    return Arr.get(target, key, default)


def data_set(target: Dict[str, Any], key: str, value: Any) -> Dict[str, Any]:
    """Write ``key`` into a nested dict using dot-notation.

    Thin alias around :meth:`cara.support.Arr.set`. Mutates ``target``
    (Laravel parity); returns the same object for chaining.
    """
    from cara.support.Arr import Arr

    return Arr.set(target, key, value)


class _OptionalProxy:
    """Wrapper around a possibly-``None`` value for safe attribute / key chains.

    Used by :func:`optional`. Mirrors Laravel's ``optional()`` —
    accessing any attribute / item on the proxy returns another
    proxy (or the underlying value when wrapped is non-None and the
    attribute exists). Calls fall through to the wrapped value or
    return another proxy when wrapped is None.
    """

    __slots__ = ("_value",)

    def __init__(self, value: Any) -> None:
        object.__setattr__(self, "_value", value)

    def __getattr__(self, name: str) -> Any:
        wrapped = object.__getattribute__(self, "_value")
        if wrapped is None:
            return _OptionalProxy(None)
        attr = getattr(wrapped, name, None)
        if attr is None:
            return _OptionalProxy(None)
        return attr

    def __getitem__(self, key: Any) -> Any:
        wrapped = object.__getattribute__(self, "_value")
        if wrapped is None:
            return _OptionalProxy(None)
        try:
            return wrapped[key]
        except (KeyError, IndexError, TypeError):
            return _OptionalProxy(None)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        wrapped = object.__getattribute__(self, "_value")
        if callable(wrapped):
            return wrapped(*args, **kwargs)
        return _OptionalProxy(None)

    def __bool__(self) -> bool:
        return bool(object.__getattribute__(self, "_value"))

    def __eq__(self, other: Any) -> bool:
        wrapped = object.__getattribute__(self, "_value")
        if isinstance(other, _OptionalProxy):
            other = object.__getattribute__(other, "_value")
        return wrapped == other

    def __repr__(self) -> str:
        return f"optional({object.__getattribute__(self, '_value')!r})"


def optional(target: Any) -> Any:
    """Wrap ``target`` so attribute / item access never raises on None.

    Mirrors Laravel's ``optional()`` helper — lets you write
    ``optional(user).profile.email`` without an outer ``if user:``
    guard. The wrapper always proxies, so a chain that produces
    ``None`` partway through (e.g. ``user.profile is None``) still
    returns a usable proxy that ``or`` falls back to a default::

        email = optional(user).profile.email or "n/a"

    Truthiness, equality, and call-through forward to the wrapped
    value, so callers can mix proxied and raw access freely::

        if optional(user).is_active:        # falls back to False when None
            ...
        assert optional(42) == 42           # equality forwards
    """
    return _OptionalProxy(target)


def dispatch(job: Any, *, routing_key: Optional[str] = None, delay: Optional[float] = None) -> Any:
    """Dispatch a queue job through ``Bus`` — Laravel global helper parity.

    Convenience over ``from cara.queues import Bus; await Bus.dispatch(...)``
    for call sites that just want a one-liner. Returns the awaitable
    so callers ``await dispatch(MyJob(...))`` directly.
    """
    from cara.queues import Bus

    kwargs: Dict[str, Any] = {}
    if routing_key is not None:
        kwargs["routing_key"] = routing_key
    if delay is not None:
        kwargs["delay"] = delay
    return Bus.dispatch(job, **kwargs)


def now(tz: str = "UTC") -> Any:
    """Return the current ``pendulum.DateTime`` (default UTC).

    Mirrors Laravel's ``now()`` helper. Routes through
    :class:`cara.support.Date` so test-suite freezes
    (``Date.set_test_now(...)`` / ``Date.travel(...)``) apply
    automatically — every place using ``now()`` becomes deterministic
    in tests without each call site needing to know about the freeze
    machinery.
    """
    from .support.Date import Date

    return Date.now(tz)


def today(tz: str = "UTC") -> Any:
    """Return today at midnight — Laravel ``today()`` parity."""
    from .support.Date import Date

    return Date.today(tz)


def yesterday(tz: str = "UTC") -> Any:
    """Return yesterday at midnight — Laravel ``yesterday()`` parity."""
    from .support.Date import Date

    return Date.yesterday(tz)


def tomorrow(tz: str = "UTC") -> Any:
    """Return tomorrow at midnight — Laravel ``tomorrow()`` parity."""
    from .support.Date import Date

    return Date.tomorrow(tz)


# ── Predicates ────────────────────────────────────────────────────────────

def blank(value: Any) -> bool:
    """Return True for ``None``, empty / whitespace strings, or empty containers.

    Mirrors Laravel's ``blank()`` helper exactly:

    * ``None`` → True
    * Empty / whitespace-only strings → True
    * Empty list / tuple / dict / set → True
    * **Numbers and booleans are NEVER blank** (``blank(0) == False``,
      ``blank(False) == False``) — Laravel parity. This is the
      property that makes ``blank()`` worth having over a bare
      ``not value`` check, which would lose ``0`` and ``False``.
    """
    if value is None:
        return True
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return False
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, tuple, dict, set, frozenset)):
        return len(value) == 0
    try:
        return not bool(value)
    except Exception:
        return False


def filled(value: Any) -> bool:
    """Inverse of :func:`blank`. Mirrors Laravel's ``filled()`` helper."""
    return not blank(value)


def head(items: Any) -> Any:
    """Return the first element of ``items``, or ``None`` if empty.

    Mirrors Laravel's ``head()``. Accepts list / tuple / dict (returns
    first value) / iterator (consumes one element).
    """
    if items is None:
        return None
    if isinstance(items, dict):
        return next(iter(items.values()), None)
    if isinstance(items, (list, tuple)):
        return items[0] if items else None
    try:
        return next(iter(items))
    except (StopIteration, TypeError):
        return None


def last(items: Any) -> Any:
    """Return the last element of ``items``, or ``None`` if empty.

    Mirrors Laravel's ``last()``. Accepts list / tuple / dict (returns
    last value) — iterators raise ``TypeError`` since they aren't
    reversible without materialisation.
    """
    if items is None:
        return None
    if isinstance(items, dict):
        if not items:
            return None
        for k in items:
            pass
        return items[k]
    if isinstance(items, (list, tuple)):
        return items[-1] if items else None
    try:
        return list(items)[-1]
    except (TypeError, IndexError):
        return None


# ── Throw / report shortcuts ──────────────────────────────────────────────

def throw_if(condition: Any, exception: Any, *args: Any, **kwargs: Any) -> Any:
    """Raise ``exception`` if ``condition`` is truthy, else return condition.

    Mirrors Laravel's ``throw_if()`` — exception may be an instance
    (raised as-is) or a class (instantiated with ``*args, **kwargs``).
    Returns the condition unchanged when not truthy so call sites
    chain naturally::

        product = throw_if(repo.find(id) is None, NotFoundError("..."))
    """
    if condition:
        if isinstance(exception, type):
            raise exception(*args, **kwargs)
        raise exception
    return condition


def throw_unless(condition: Any, exception: Any, *args: Any, **kwargs: Any) -> Any:
    """Raise ``exception`` unless ``condition`` is truthy. Mirrors Laravel."""
    if not condition:
        if isinstance(exception, type):
            raise exception(*args, **kwargs)
        raise exception
    return condition


def report(exception: BaseException) -> None:
    """Log ``exception`` to the structured logger without re-raising.

    Mirrors Laravel's ``report()`` — useful in best-effort branches
    where the failure should surface in monitoring (Sentry,
    structured logs) without aborting the request / job. Falls back
    to ``stderr`` if the Log facade itself fails.
    """
    try:
        from cara.facades import Log

        Log.error(
            f"{exception.__class__.__name__}: {exception}",
            context={"exception_type": exception.__class__.__name__},
        )
    except Exception as log_err:
        import sys as _sys

        print(
            f"[cara.helpers.report] Log facade failed ({log_err}); "
            f"original={exception.__class__.__name__}: {exception}",
            file=_sys.stderr,
        )


def report_if(condition: Any, exception: BaseException) -> Any:
    """``report(exception)`` only if ``condition`` is truthy. Returns condition."""
    if condition:
        report(exception)
    return condition


# ── Container / facade access ─────────────────────────────────────────────

def app(name: Optional[str] = None) -> Any:
    """Resolve ``name`` from the IoC container, or return the container itself.

    Mirrors Laravel's ``app()`` global. Without ``name``, returns the
    bootstrap ``application``; with ``name``, returns
    ``application.make(name)``.
    """
    from bootstrap import application

    if name is None:
        return application
    return application.make(name)


def event(event_obj: Any) -> Any:
    """Fire ``event_obj`` through the Event dispatcher.

    Mirrors Laravel's ``event()`` global. Returns the awaitable from
    ``Event.fire`` so callers ``await event(MyEvent(...))`` directly.
    """
    from cara.facades import Event

    return Event.fire(event_obj)


def cache() -> Any:
    """Return the Cache facade — Laravel ``cache()`` parity.

    Useful as a one-liner alternative to ``from cara.facades import Cache``
    when a function only needs the facade once::

        cached = cache().remember(key, ttl, callback)
    """
    from cara.facades import Cache

    return Cache


def logger() -> Any:
    """Return the Log facade — Laravel ``logger()`` parity."""
    from cara.facades import Log

    return Log


def auth(guard: Optional[str] = None) -> Any:
    """Resolve the auth manager (or a specific guard).

    Mirrors Laravel's ``auth()`` / ``auth('api')`` overload. Returns
    the auth manager when no guard is given; otherwise returns the
    named guard.
    """
    from bootstrap import application

    auth_manager = application.make("auth")
    if guard is None:
        return auth_manager
    return auth_manager.guard(guard)


def bcrypt(password: str, *, rounds: int = 12) -> str:
    """Hash ``password`` using bcrypt — Laravel ``bcrypt()`` parity.

    Resolves the cara ``Hash`` facade so cost-rounds + algorithm
    selection live with the rest of the encryption stack rather than
    being hard-coded at every call site.
    """
    from cara.encryption import Hash

    return Hash.make(password, rounds=rounds)


# ── Misc ──────────────────────────────────────────────────────────────────

def class_basename(class_or_obj: Any) -> str:
    """Return the unqualified class name — Laravel ``class_basename`` parity.

    Accepts a class or any instance::

        class_basename(SomeService)      == "SomeService"
        class_basename(some_service())   == "SomeService"
        class_basename("a.b.C")          == "C"   # dotted-string also works
    """
    if isinstance(class_or_obj, str):
        return class_or_obj.rsplit(".", 1)[-1].rsplit("\\", 1)[-1]
    cls = class_or_obj if isinstance(class_or_obj, type) else class_or_obj.__class__
    return cls.__name__


def e(text: Any) -> str:
    """HTML-escape ``text`` — Laravel ``e()`` parity.

    Coerces ``None`` to empty string. Quote-character escaping uses
    the same rules as ``html.escape(quote=True)`` (``&`` ``<`` ``>``
    ``"`` ``'``).
    """
    import html

    from .support.HtmlString import HtmlString

    if text is None:
        return ""
    # HtmlString marker → already-trusted markup, pass through verbatim.
    if isinstance(text, HtmlString):
        return text.to_html()
    return html.escape(str(text), quote=True)


def collect(items: Any = None) -> Any:
    """Wrap ``items`` in a Laravel ``Collection`` — global helper parity.

    Re-exports ``cara.support.collect`` at the canonical helper path
    so application code can do ``from cara.helpers import collect``
    alongside the rest of the Laravel-style globals.
    """
    from cara.support import collect as _collect

    return _collect(items)


__all__ = [
    "abort",
    "abort_if",
    "abort_unless",
    "app",
    "auth",
    "bcrypt",
    "blank",
    "cache",
    "class_basename",
    "collect",
    "config",
    "data_get",
    "data_set",
    "dispatch",
    "e",
    "env",
    "event",
    "filled",
    "head",
    "last",
    "logger",
    "now",
    "today",
    "tomorrow",
    "yesterday",
    "optional",
    "report",
    "report_if",
    "route",
    "safe_call",
    "tap",
    "throw_if",
    "throw_unless",
    "value",
]
