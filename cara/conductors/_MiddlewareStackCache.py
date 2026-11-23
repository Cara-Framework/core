"""Resolved middleware stacks, kept for as long as their capsule is unchanged."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from cara.exceptions import MiddlewareNotFoundException
from cara.middleware import MiddlewareCapsule

#: Key of the global stack; route stacks are keyed on their declaration tuple,
#: which can never be this object.
_GLOBAL = object()


class _MiddlewareStackCache:
    """Ordered middleware stacks of one conductor, by declaration.

    A route declares its middleware once and the capsule is filled at boot,
    yet every request resolved each alias and rebuilt the priority index to
    sort the same stack again (~3 us for a five-entry declaration, ~0.25 us
    once kept). Stacks are kept per DECLARATION (routes that declare the same
    list share one entry) plus one for the global stack, and are served only
    while the capsule's registration ``version`` is the one they were
    resolved at: every registration method bumps it, so a late ``add_alias``
    / ``add_to_group`` / ``set_priority`` / ``remove`` is honoured by the very
    next request at the cost of one integer compare.

    A capsule that is not exactly ``MiddlewareCapsule`` (a subclass or a
    double may resolve from state no version covers) is asked every time.
    Every caller gets a fresh list: a pipeline keeps the one it is handed.
    """

    def __init__(self) -> None:
        self._capsule: Any = None
        self._version: int = -1
        self._stacks: dict[Any, tuple] = {}

    def global_stack(self, capsule: Any) -> list:
        """``capsule.get_global_middleware()``, resolved once per version."""
        return self._get(capsule, _GLOBAL, capsule.get_global_middleware)

    def route_stack(self, capsule: Any, declared: Any) -> list:
        """The resolved, priority-sorted stack for a route's declaration."""
        try:
            key = tuple(declared)
            hash(key)
        except TypeError:
            # An unhashable declaration cannot be a key; resolve it every time.
            return self.resolve_route_middleware(capsule, list(declared))
        return self._get(
            capsule, key, lambda: self.resolve_route_middleware(capsule, key)
        )

    def _get(self, capsule: Any, key: Any, resolve: Callable[[], list]) -> list:
        if type(capsule) is not MiddlewareCapsule:
            return list(resolve())
        version = capsule.version
        if capsule is not self._capsule or version != self._version:
            self._capsule, self._version, self._stacks = capsule, version, {}
        stack = self._stacks.get(key)
        if stack is None:
            # A failed resolution raises before anything is stored, so a miss
            # (an alias registered later) is never remembered.
            stack = self._stacks[key] = tuple(resolve())
        return list(stack)

    @staticmethod
    def resolve_route_middleware(capsule: Any, declared: Any) -> list:
        """Resolve and order a route's declared middleware through the capsule."""
        route_middleware = []
        for mw in declared:
            resolved = capsule.resolve_middleware(mw)
            if resolved is None:
                raise MiddlewareNotFoundException(
                    f"Middleware alias or group '{mw}' could not be resolved."
                )
            if isinstance(resolved, list):
                route_middleware.extend(resolved)
            else:
                route_middleware.append(resolved)
        return capsule.sort_by_priority(route_middleware)
