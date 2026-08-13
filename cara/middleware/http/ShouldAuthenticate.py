"""
Base Authentication Middleware for Cara Framework

Core authentication logic with easy customization points.
Users can extend this in their app for custom authentication needs.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from cara.exceptions import CaraException
from cara.http import Request, Response
from cara.middleware.Middleware import Middleware


def denial_status(error: Exception) -> int | None:
    """The 4xx status an exception DENIES with, or ``None`` if it is a fault.

    The single predicate for "this guard said no" — ``handle`` uses it to
    decide what to swallow and ``authentication_failed`` uses it to decide
    what it is allowed to render. Stated once because the two halves drifted
    apart before: the loop narrowed to ``is_http_exception`` while the
    renderer still keyed on "has an int ``status_code``", so
    ``DatabaseUnavailableException`` — HTTP-facing, ``status_code = 503``,
    raised by ``PostgresConnection`` as ``DatabaseUnavailableException(str(e))``
    around a psycopg ``OperationalError`` — passed the loop as a denial and
    was rendered by hand as::

        503 {"error": "connection to server at \\"db-prod\\" (10.4.2.11), port
             5432 failed: FATAL:  password authentication failed for user
             \\"cara_api\\"", "type": "authentication_error"}

    to an unauthenticated caller, in production, with debug off. That body
    never reaches ``DefaultExceptionHandler.format_response``, so the
    ``status_code >= 500 and not debug`` redaction that exists precisely for
    this string never ran, and the documented ``retry_after`` contract (plus
    its ``Retry-After`` header) was dropped on the way.

    A 4xx is the only outcome a guard can *assert*: the caller's credentials
    were absent, malformed, expired, locked out. Everything else — 5xx, no
    status at all, an arbitrary driver exception — means the guard never got
    far enough to judge the credentials, and §9 forbids answering "your
    credentials failed" when the truth is "we failed".

    Keyed on the declared status rather than on ``is_http_exception`` so an
    application exception that owns a 4xx and a ``to_dict`` (for example a
    re-authentication demand answering 403) is honoured without having to
    inherit from a framework base.
    """
    status = getattr(error, "status_code", None)
    if not isinstance(status, int) or isinstance(status, bool):
        return None
    return status if 400 <= status < 500 else None


class ShouldAuthenticate(Middleware):
    """Base authentication middleware with automatic parameter parsing."""

    def __init__(self, application, guards: list[str] | None = None):
        super().__init__(application)

        if guards:
            self.guards = list(guards)
            return

        # Missing auth wiring is a boot/configuration failure. Inventing a
        # ``jwt`` default here can authenticate with a guard the application
        # did not select.
        auth_manager = application.make("auth")
        self.guards = [auth_manager.get_default_guard()]

    async def handle(
        self, request: Request, next_fn: Callable[[Any], Awaitable[Any]]
    ) -> Response:
        """Handle authentication check."""
        # Check if authentication should be skipped
        if self.should_skip_authentication(request):
            return await next_fn(request)

        # Try to authenticate with each guard until one succeeds
        user = None
        successful_guard = None
        last_error = None

        for guard_name in self.guards:
            try:
                auth_manager = self.application.make("auth")
                guard = auth_manager.guard(guard_name)

                # Let guard handle its own authentication and error messages
                user = guard.user()
                if user:
                    successful_guard = guard_name
                    break

            # Only a 4xx taxonomy exception means "this guard says no"
            # (AuthenticationException and friends, plus
            # AccountLockedException's 429). A bare ``except Exception`` here
            # swallowed pool exhaustion, a Redis timeout in the token
            # blacklist, an ImportError on the JWT library — and shipped the
            # raw ``str(exception)`` as a 401 body, never passing through
            # DefaultExceptionHandler's production redaction for unexpected
            # 5xx, so a psycopg error carrying the DSN or the failing SQL went
            # verbatim to an unauthenticated caller. A database outage also
            # presented to clients and dashboards as "401
            # authentication_error" while every 5xx alert stayed silent.
            # Narrowing to ``is_http_exception`` was not enough: the
            # framework's own ``DatabaseUnavailableException`` IS HTTP-facing
            # and carries the psycopg text at 503. ``denial_status`` is the
            # SSOT for the distinction. Anything else is re-raised for the
            # global handler, exactly as ``CanPerform`` does for the
            # authorization half.
            except CaraException as e:
                if denial_status(e) is None:
                    raise
                last_error = e
                continue

        if not user:
            return self.authentication_failed(request, last_error)

        # ``Request.user`` is a method (returns ``self._user``).
        # Assigning ``request.user = user`` shadows the method on the
        # instance — every subsequent ``request.user()`` then raises
        # ``TypeError: 'User' object is not callable``. Use the
        # documented setter so ``request.user()`` keeps working and
        # downstream code (controllers, facades, ResetAuth) sees one
        # canonical place where the per-request user lives.
        request.set_user(user)
        request._route_auth_guard = successful_guard

        response = await next_fn(request)
        return response

    def authentication_failed(
        self, request: Request, last_error: Exception | None = None
    ) -> Response:
        """Render a DENIAL. A fault is re-raised for the envelope system.

        Canonical error shape: ``{error, type, ...}`` (see
        ``HttpException.to_dict``). Pre-fix this middleware used
        ``{error: "Unauthorized", message: "..."}``, which broke every client
        that branches on ``type``.

        Three corrections, all the same mistake — inventing an answer the
        guard never gave:

        * The typed branch used to require BOTH ``.message`` and
          ``.status_code``. ``CaraException`` carries neither — only
          ``status_code`` is declared on the HTTP-facing subclasses — so the
          branch was dead for the framework's own taxonomy and every typed
          failure was flattened to 401. ``AccountLockedException`` in
          particular lost its 429 and its ``retry_after``. Gate on the
          declared status and let the exception render its own body via
          ``to_dict`` where it has one.
        * That gate then accepted ANY int status, including 5xx, and
          hand-rolled the body. ``DatabaseUnavailableException`` — the class
          ``PostgresConnection`` raises with the raw psycopg text — was
          therefore answered as a 503 quoting the connection host and the
          ``password authentication failed for user`` line, to an
          unauthenticated caller, with debug off. The redaction that exists
          for exactly that string lives in
          ``DefaultExceptionHandler.format_response``, and no response built
          here ever reaches it. §9: this method does not write 5xx bodies.
          It re-raises, the envelope system answers, the 5xx alert fires and
          ``retry_after`` survives into the ``Retry-After`` header.
        * The catch-all echoed ``str(last_error)``. A bare ``CaraException``
          has no declared status precisely because it is an internal fault —
          ``QueryException`` carries the failing SQL — so echoing it at 401
          was the same leak one branch down. Gone with the branch.

        ``RuntimeError`` remains the one message the framework will speak on
        a caller's behalf. It cannot arrive from a guard (``handle`` only
        swallows ``denial_status`` matches), so it is unambiguously a DIRECT
        caller — ``AuthenticateUser`` and application session middleware pass
        ``RuntimeError("Session revoked or expired")`` and their clients
        branch on that 401. The message is chosen by the caller, not produced
        by a driver. New callers should raise a 4xx taxonomy exception
        instead; this channel is not extended.
        """
        response = Response(self.application)

        if last_error is None:
            # No guard produced an error — pre-auth state (no credentials).
            return response.json(
                {
                    "error": "Authentication required",
                    "type": "authentication_error",
                },
                401,
            )

        status_code = denial_status(last_error)
        if status_code is not None:
            # A denial: the exception owns both its status and its body.
            to_dict = getattr(last_error, "to_dict", None)
            if callable(to_dict):
                return response.json(to_dict(), status_code)
            message = getattr(last_error, "message", None) or str(last_error)
            return response.json(
                {
                    "error": message or "Unauthorized",
                    "type": "authentication_error",
                },
                status_code,
            )

        if type(last_error) is RuntimeError:
            # Caller-authored denial text (see the docstring). Exact type,
            # not ``isinstance``: RuntimeError is the base of a long tail of
            # library faults, and a subclass is never a deliberate message.
            return response.json(
                {
                    "error": str(last_error) or "Unauthorized",
                    "type": "authentication_error",
                },
                401,
            )

        # Not a denial — the guard crashed before it could judge the
        # credentials. Answering 401 would tell the caller their credentials
        # failed and would hide the outage from every 5xx alert; answering a
        # hand-rolled 5xx would skip the redaction. Hand it to the handler.
        raise last_error

    def should_skip_authentication(self, request: Request) -> bool:
        """Determine if authentication should be skipped for this request."""
        return False
