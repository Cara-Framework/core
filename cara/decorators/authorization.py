"""Authorization decorators for controller methods (Laravel-style).

    @can("admin")
    @can("update", some_model)      # extra args are forwarded to the Gate
    @can_any(["admin", "editor"])
    @admin_only
    @authenticated_only
    @guest_only

Every decorator works on both sync and async handlers. On denial it raises
``AuthorizationFailedException`` (HTTP 403, or 401 for the auth/guest guards),
which the framework's exception handler renders as a JSON error response.

The decorator resolves the user from the request argument when present
(``request.user()``) and otherwise from the ``Auth`` facade, then evaluates the
check against ``Gate.for_user(user)`` so the result matches the request's
identity exactly.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any

from cara.exceptions import AuthorizationFailedException
from cara.facades import Auth, Gate


def _resolve_user(func_args: tuple) -> Any | None:
    """Resolve the acting user from the wrapped handler's arguments.

    Handlers are either ``(self, request, ...)`` for controller methods or
    ``(request, ...)`` for plain functions. We look for the first argument that
    exposes a ``user()`` method; failing that we fall back to ``Auth.user()``.
    """
    for candidate in func_args[:2]:
        user_fn = getattr(candidate, "user", None)
        if callable(user_fn):
            try:
                return user_fn()
            except (ImportError, RuntimeError, AttributeError):  # noqa: BLE001 — fall through to the Auth facade
                break
    try:
        return Auth.user()
    except (ImportError, RuntimeError, AttributeError):  # noqa: BLE001 — treat an unresolved user as a guest
        return None


def _guard(check: Callable[[tuple], None]) -> Callable:
    """Build a decorator that runs ``check(args)`` before the handler.

    Preserves the handler's sync/async nature so frameworks that introspect
    ``iscoroutinefunction`` keep working.
    """

    def decorator(func: Callable) -> Callable:
        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                check(args)
                return await func(*args, **kwargs)

            return async_wrapper

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            check(args)
            return func(*args, **kwargs)

        return sync_wrapper

    return decorator


def can(ability: str, *args: Any) -> Callable:
    """Require a single ability."""

    def check(func_args: tuple) -> None:
        user = _resolve_user(func_args)
        if Gate.for_user(user).denies(ability, *args):
            raise AuthorizationFailedException(
                message=f"This action is unauthorized. Missing ability: {ability}",
                ability=ability,
                user=user,
                status_code=403,
            )

    return _guard(check)


def can_any(abilities: list[str], *args: Any) -> Callable:
    """Require any one of several abilities (OR logic)."""

    def check(func_args: tuple) -> None:
        user = _resolve_user(func_args)
        if not Gate.for_user(user).any(abilities, *args):
            joined = ", ".join(abilities)
            raise AuthorizationFailedException(
                message=f"This action requires any of: {joined}",
                ability=joined,
                user=user,
                status_code=403,
            )

    return _guard(check)


def authorize(ability: str, *args: Any) -> Callable:
    """Alias of :func:`can`."""
    return can(ability, *args)


def admin_only(func: Callable) -> Callable:
    """Shorthand for ``@can("admin")``."""
    return can("admin")(func)


def authenticated_only(func: Callable) -> Callable:
    """Require an authenticated user."""

    def check(func_args: tuple) -> None:
        if _resolve_user(func_args) is None:
            raise AuthorizationFailedException(
                message="Authentication required.",
                status_code=401,
            )

    return _guard(check)(func)


def guest_only(func: Callable) -> Callable:
    """Require that there is no authenticated user."""

    def check(func_args: tuple) -> None:
        if _resolve_user(func_args) is not None:
            raise AuthorizationFailedException(
                message="This action is only available to guests.",
                status_code=403,
            )

    return _guard(check)(func)
