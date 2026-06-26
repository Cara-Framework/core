"""Authentication helpers — request user resolution.

Project-agnostic framework utility: resolve the authenticated user (and its
id) off the request object that the ``AuthenticateUser`` middleware populated
via ``request.set_user(...)``. The per-app "what happens when there is no
user" policy (HTTP abort vs domain exception) stays in the app layer.
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


__all__ = [
    "optional_user_id",
    "resolve_user",
]
