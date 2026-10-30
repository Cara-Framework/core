"""
Path Redaction — no capability carried in a URL path reaches a request log.

Some routes carry their secret IN the URL path: whoever holds the URL holds
the door, with no session behind it (a webhook secret, a shareable link, a
download link). ``LogHttpRequests`` redacts secret QUERY parameters by name,
but a path segment has no name on the wire — only the route table says which
segment is a secret. So every route parameter named in
``logging.secret_path_parameters`` is written as
:attr:`PathRedaction.REDACTED_SEGMENT` by every framework request log:
``LogHttpRequests`` (stdout and the log files), ``RecordRequestMetrics`` (the
slow-request warning) and ``PersistRequestLog`` (the durable row).

The path is matched against each secret route's PATTERN whatever the method,
so a GET at a POST-only webhook (a 405 the router answers before any route is
set on the request) is redacted too. The booted router's secret routes are
read once per router: every route registers at boot, before the first request
is served.

Configuration (read at call time, never snapshotted at import):

``logging.secret_path_parameters``
    Route parameter names whose value is a capability. Default
    :attr:`PathRedaction.DEFAULT_SECRET_PARAMETERS`; a configured list
    REPLACES it.
"""

from __future__ import annotations

from collections.abc import Iterable
from functools import lru_cache
from typing import Any

from cara.configuration import config
from cara.facades import Log

#: ``(route, segment count, secret segment positions)``.
_SecretRoute = tuple[Any, int, tuple[int, ...]]


class PathRedaction:
    """Redact the capability segments of a request path before it is logged."""

    #: Route parameters whose value IS a capability unless configured otherwise.
    DEFAULT_SECRET_PARAMETERS: tuple[str, ...] = ("token",)

    #: What a secret path segment is written as. Also the whole logged path
    #: when the redaction itself cannot run (fail closed).
    REDACTED_SEGMENT = "[redacted]"

    #: Log category of the fail-closed warning: the access log's own.
    LOG_CATEGORY = "cara.http.requests"

    @classmethod
    def loggable_path(cls, path: str, application: Any) -> str:
        """``path`` as a request log may keep it.

        Never raises — a request log runs on every request and must not fail
        it. When the redaction cannot run, the whole path is withheld rather
        than written raw.
        """
        if not path:
            return path
        try:
            routes = _secret_routes_of(
                application.make("router"), cls.secret_parameters()
            )
            return cls.redact(path, routes)
        except Exception as exc:
            Log.warning(
                f"request path withheld from the log, redaction failed: {exc}",
                category=cls.LOG_CATEGORY,
            )
            return cls.REDACTED_SEGMENT

    @classmethod
    def secret_parameters(cls) -> frozenset[str]:
        """Resolve ``logging.secret_path_parameters`` at call time."""
        configured = config("logging.secret_path_parameters", None)
        if configured is None:
            return frozenset(cls.DEFAULT_SECRET_PARAMETERS)
        if isinstance(configured, str):
            return frozenset({configured})
        try:
            return frozenset(str(name) for name in configured)
        except TypeError:
            return frozenset(cls.DEFAULT_SECRET_PARAMETERS)

    @staticmethod
    def secret_routes(
        routes: Iterable[Any], parameters: Iterable[str]
    ) -> tuple[_SecretRoute, ...]:
        """``(route, segment count, secret segment positions)`` for every
        route whose URL binds one of ``parameters``."""
        names = frozenset(parameters)
        found = []
        for route in routes:
            parts = [
                part for part in str(getattr(route, "url", "") or "").split("/") if part
            ]
            positions = tuple(
                index
                for index, part in enumerate(parts)
                if _parameter_name(part) in names
            )
            if positions:
                found.append((route, len(parts), positions))
        return tuple(found)

    @classmethod
    def redact(cls, path: str, secret_routes: Iterable[_SecretRoute]) -> str:
        """``path`` with each secret segment replaced by
        :attr:`REDACTED_SEGMENT` when a :meth:`secret_routes` pattern matches
        it; any other path is returned unchanged."""
        segments = [segment for segment in path.split("/") if segment]
        normalized = "/" + "/".join(segments)
        for route, width, positions in secret_routes:
            if not route.compiler.matches(normalized):
                continue
            if len(segments) != width:
                # An optional parameter left out (or a multi-segment one)
                # shifts the segments; the route's own pattern carries no
                # value at all.
                return str(route.url)
            for position in positions:
                segments[position] = cls.REDACTED_SEGMENT
            return "/" + "/".join(segments)
        return path


def _parameter_name(part: str) -> str | None:
    """The parameter one route URL segment binds (``@token``, ``@id:int``,
    ``@page?``), or None for a literal segment."""
    if not part.startswith("@"):
        return None
    return part[1:].removesuffix("?").partition(":")[0]


@lru_cache(maxsize=8)
def _secret_routes_of(
    router: Any, parameters: frozenset[str]
) -> tuple[_SecretRoute, ...]:
    """:meth:`PathRedaction.secret_routes` of the booted router, read once
    per router and parameter set."""
    return PathRedaction.secret_routes(router.routes, parameters)
