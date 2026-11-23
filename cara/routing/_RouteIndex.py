"""Lookup index over one method bucket's routes, by leading literal segments.

``Router.find`` used to try every route of the request's method in
registration order, one compiled regex after another, until one matched. A
product registers ~330 GET routes, so a request for a route declared late paid
~300 failed regex matches first (44 us measured, 19 us for one in the middle),
and a 404 or 405 paid for EVERY route of EVERY method (183 us) — which is the
request an unauthenticated scanner sends.

The index narrows the candidates without changing which route wins:

* A route's pattern is ``^/lit/lit/…`` followed by its first parameter, so a
  path can match it only if the path starts with exactly those literal
  segments (``RouteCompiler.static_prefix``). Routes are filed in a trie under
  that prefix; a route that starts with a parameter (or is ``/``) is filed at
  the root and is a candidate for every path.
* Looking up a path walks the trie along the path's own segments and gathers
  the routes filed at every node passed. That set is a SUPERSET of the routes
  whose regex can match, and it is tried in REGISTRATION ORDER — so the first
  match is the same route the full scan would have returned. Precedence
  between a static route and a parameter route that both match stays "first
  registered wins".

The regex still decides every match; the index only skips routes that cannot.
"""

from __future__ import annotations

from operator import itemgetter
from typing import Any

from .Route import Route
from .RouteCompiler import RouteCompiler

_BY_ORDER = itemgetter(0)


class _PrefixNode:
    __slots__ = ("children", "routes")

    def __init__(self) -> None:
        self.children: dict[str, _PrefixNode] = {}
        #: ``(registration order, route)`` of the routes whose literal prefix
        #: ends exactly here, in registration order.
        self.routes: list[tuple[int, Any]] = []


class _RouteIndex:
    """The routes of one method bucket, reachable by their literal prefix."""

    __slots__ = ("bucket", "recompiles", "root", "size")

    def __init__(self, bucket: list[Any]) -> None:
        self.bucket = bucket
        self.size = len(bucket)
        self.recompiles = RouteCompiler.recompiles
        self.root = _PrefixNode()
        for order, route in enumerate(bucket):
            node = self.root
            for segment in self._literal_prefix(route):
                child = node.children.get(segment)
                if child is None:
                    child = node.children[segment] = _PrefixNode()
                node = child
            node.routes.append((order, route))

    def is_current_for(self, bucket: list[Any]) -> bool:
        """Whether the bucket and its patterns are still what was indexed.

        Routes are appended at boot (``Router.add``) and a registered route's
        pattern only changes through ``RouteCompiler.compile_route`` (a group
        re-prefix), which bumps ``RouteCompiler.recompiles``.
        """
        return (
            bucket is self.bucket
            and len(bucket) == self.size
            and RouteCompiler.recompiles == self.recompiles
        )

    def candidates(self, path: str) -> list[tuple[int, Any]]:
        """``(order, route)`` for every route that could match ``path``, in
        registration order."""
        node = self.root
        found = node.routes
        merged: list[tuple[int, Any]] | None = None
        if path.startswith("/"):
            for segment in path.split("/")[1:]:
                node = node.children.get(segment)
                if node is None:
                    break
                if not node.routes:
                    continue
                if not found:
                    found = node.routes
                else:
                    if merged is None:
                        merged = list(found)
                    merged.extend(node.routes)
        if merged is None:
            return found
        merged.sort(key=_BY_ORDER)
        return merged

    @staticmethod
    def _literal_prefix(route: Any) -> tuple[str, ...]:
        """The literal segments ``route`` is filed under.

        Only the stock ``Route.matches`` over the stock ``RouteCompiler``
        promises the pattern shape the index relies on. A route that matches
        any other way is filed at the root — a candidate for every path,
        exactly as before the index existed.
        """
        compiler = getattr(route, "compiler", None)
        if type(compiler) is not RouteCompiler:
            return ()
        if getattr(type(route), "matches", None) is not Route.matches:
            return ()
        return compiler.static_prefix
