"""Authentication helpers — request user resolution and the 401 policy.

Project-agnostic framework utility: resolve the authenticated user (and its
id) off the request object that the ``AuthenticateUser`` middleware populated
via ``request.set_user(...)``.

Two flavours, because routes come in two kinds:

* **Optional** (:func:`resolve_user`, :func:`optional_user_id`) — for public
  routes that merely personalise a response when someone happens to be signed
  in. These never raise.
* **Required** (:func:`authenticated_user`, :func:`user_id`) — for routes
  behind the ``auth`` middleware, where the middleware has *already*
  guaranteed a user. Reaching these without one means routing is
  misconfigured, so they :func:`~cara.helpers.abort` with 401 rather than let
  an unauthenticated request continue in an authenticated code path. That is
  the framework's answer for a framework-owned invariant: the required
  variants return a plain value, so call sites never re-check and never grow
  their own divergent "no user" branches.

:func:`gate_allows` is the same shape for authorization: a never-raising
"does the request's user pass this ability?" over the :class:`Gate`, with the
ability name left as a parameter so no policy vocabulary lands in the
framework.
"""

from __future__ import annotations

import logging
from typing import Any

_logger = logging.getLogger("cara.support.Auth")


def resolve_user(request: Any) -> Any | None:
    """Return the authenticated user via ``request.user()`` / ``_user``.

    ``Request.user`` is a *method* (returns ``self._user``), so reading
    ``request.user`` yields a bound method — truthy AND callable — never the
    user object.  Call it (or fall back to ``_user``) to get what
    ``AuthenticateUser`` middleware stored via ``request.set_user(...)``.
    """
    try:
        user_fn = getattr(request, "user", None)
        if callable(user_fn):
            return user_fn()
    except Exception:
        _logger.warning("Failed to resolve user from request.user()", exc_info=True)
    return getattr(request, "_user", None)


def optional_user_id(request: Any) -> int | None:
    """Return the user id if present, else ``None``.

    For public routes that personalize the response when a user happens to be
    signed in (search ranking, browsing history, homepage).  Never raises —
    the route is reachable without auth.
    """
    user = resolve_user(request)
    if user is None or not hasattr(user, "id"):
        return None
    return user.id


def authenticated_user(request: Any) -> Any:
    """Return the authenticated user object, or abort with 401.

    Use on routes the ``auth`` middleware protects: it has already called
    ``request.set_user(...)`` and aborted when no user resolved, so a missing
    (or id-less) user here means the route is wired wrong. Failing hard beats
    continuing with unauthenticated context, and returning a non-optional
    value keeps defensive ``if not user`` branches out of every call site.
    """
    user = resolve_user(request)
    if user is None or not hasattr(user, "id") or user.id is None:
        # Imported lazily: ``cara.helpers`` pulls configuration/environment,
        # which must not be dragged in merely by importing ``cara.support``.
        from cara.helpers import abort  # local: cycle with cara.helpers

        abort(401, "Authentication required")
    return user


def user_id(request: Any) -> int:
    """Return the authenticated user's id on an auth-protected route.

    The non-optional counterpart of :func:`optional_user_id`: aborts with 401
    (via :func:`authenticated_user`) rather than returning ``None``, so call
    sites can use the id directly.
    """
    return authenticated_user(request).id


def gate_allows(request: Any, ability: str) -> bool:
    """``True`` when the request's user passes ``ability`` on the Gate.

    Authorization has one definition — the ability registered with the
    :class:`Gate` — and this reads it for an already-resolved request user via
    ``for_user`` (no re-authentication round-trip). Never raises: an
    unauthenticated request simply does not pass, which is what every
    "is this user allowed to X?" call site wants.

    The ability name is a parameter on purpose: which abilities exist is
    application policy, not framework vocabulary.
    """
    # Lazy: facade bindings are resolved at boot, after ``cara.support`` has
    # already been imported.
    from cara.facades import Gate  # local: cycle with cara.facades

    return Gate.for_user(resolve_user(request)).allows(ability)


__all__ = [
    "authenticated_user",
    "gate_allows",
    "optional_user_id",
    "resolve_user",
    "user_id",
]
