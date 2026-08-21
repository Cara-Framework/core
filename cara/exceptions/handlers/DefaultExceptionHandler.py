"""
Default Exception Handler.

Professional exception handler using proper exception hierarchy.
"""

from __future__ import annotations

import contextlib
import importlib
import json
import traceback
from typing import Any

from ._ExceptionResponseHeaders import _EXCEPTION_RESPONSE_HEADERS


class DefaultExceptionHandler:
    """
    Professional exception handler using exception class hierarchy.
    """

    def __init__(self, application=None):
        self.application = application

    async def handle(
        self,
        exception: Exception,
        request: Any,
        scope: dict[str, Any],
        receive: Any,
        send: Any,
    ) -> None:
        """Main entry point - handles exception properly."""
        self.log_exception(exception)
        status_code = self.get_status_code(exception)
        response_data = self.format_response(exception, status_code)
        await self.send_response(
            response_data, status_code, scope, receive, send, request
        )

    def get_status_code(self, exception: Exception) -> int:
        """Get HTTP status code from exception - Laravel style."""
        # First check instance attribute (for dynamic setting)
        if hasattr(exception, "status_code") and exception.status_code is not None:
            return exception.status_code

        # Then check class attribute (Laravel style)
        if (
            hasattr(exception.__class__, "status_code")
            and exception.__class__.status_code is not None
        ):
            return exception.__class__.status_code

        # Default to 500 for unknown exceptions
        return 500

    # Keys preserved on a redacted 5xx-in-prod response when an
    # exception's ``to_dict`` returned them. ``retry_after`` is the
    # documented 503 contract field (``ServiceUnavailableException``
    # surfaces it in both the JSON body and the ``Retry-After`` header)
    # — stripping it would force every client into blind backoff.
    # Everything else in a 5xx-prod body is replaced by the generic
    # ``error`` / ``type`` pair regardless of how it was produced.
    _5XX_PROD_SAFE_KEYS = frozenset({"retry_after"})

    def format_response(self, exception: Exception, status_code: int) -> dict[str, Any]:
        """Format the exception into a response.

        The ``to_dict`` short-circuit used to bypass the prod-5xx
        redaction policy: ``raise HttpException("DSN=postgresql://...",
        status_code=500)`` shipped the raw message + class name + every
        custom kwarg straight into the response body, because
        ``HttpException`` defines ``to_dict``. ``_GENERIC_5XX_MESSAGE``
        applied only to exceptions WITHOUT ``to_dict`` — exactly
        inverted relative to the risk.

        Now the redaction is applied uniformly: ``to_dict`` still
        produces the 4xx body (the caller acted on bad input and needs
        the real message + any context the exception attached), but
        any 5xx-in-prod response is collapsed to the generic envelope
        plus a small allowlist of contract fields (``retry_after``).
        """
        if hasattr(exception, "to_dict") and callable(exception.to_dict):
            response = exception.to_dict()
        else:
            response = self.format_error(exception, status_code)

        # Propagate the documented ``retry_after`` contract from
        # exceptions that set it but don't define ``to_dict``.
        # ``ServiceUnavailableException`` inherits ``HttpException.to_dict``
        # which scans ``__dict__`` and picks the attribute up
        # accidentally; ``DatabaseUnavailableException`` (raised by
        # ``PostgresConnection`` on pool exhaustion / connection drop)
        # inherits from ``ORMException`` which has no ``to_dict`` —
        # so the ``retry_after`` value the constructor stashes was
        # silently dropped by ``format_error``. The 5xx-prod redaction
        # below preserves ``retry_after`` from ``response`` via the
        # ``_5XX_PROD_SAFE_KEYS`` allowlist, so making sure it's in
        # ``response`` here is the single fix needed for both paths.
        retry_after = getattr(exception, "retry_after", None)
        if retry_after is not None and "retry_after" not in response:
            with contextlib.suppress(TypeError, ValueError):
                response["retry_after"] = int(retry_after)

        # Propagate the per-route allow-list from
        # ``MethodNotAllowedException`` so ``send_response`` can emit
        # the RFC 9110 §15.5.6 ``Allow`` header. Mirrors the
        # ``retry_after`` lift above. ``HttpException.to_dict`` already
        # surfaces ``allowed`` via its ``__dict__`` scan when the kwarg
        # was passed, so this only matters for callers that override
        # ``to_dict`` and don't include it — defensive against drift.
        allowed = getattr(exception, "allowed", None)
        if allowed is not None and "allowed" not in response:
            with contextlib.suppress(TypeError):
                response["allowed"] = list(allowed)

        if status_code >= 500 and not self.is_debug_mode():
            redacted: dict[str, Any] = {
                "error": self._GENERIC_5XX_MESSAGE,
                "type": self._GENERIC_5XX_TYPE,
            }
            for key in self._5XX_PROD_SAFE_KEYS:
                if key in response:
                    redacted[key] = response[key]
            return redacted

        return response

    # Generic message for unexpected 5xx errors when not in debug. The real
    # exception still hits the logs (with exc_info) — we just don't ship
    # internals (SQL errors, file paths, lib stack frames) to the caller.
    _GENERIC_5XX_MESSAGE = "Internal server error"

    # Machine-readable ``type`` tokens for the generic-error path
    # (exceptions that don't define ``to_dict``). The contract: every
    # error response carries a stable ``type`` string the client can
    # branch on, so client / SDK code doesn't have to substring-
    # match human-readable ``error`` text. Typed framework exceptions
    # (``AuthorizationException`` et al.) keep emitting their own
    # specific ``type`` via their ``to_dict``; this default only
    # covers the catch-all path.
    #
    # 5xx in production is deliberately collapsed to ``internal_error``
    # — same redaction principle as ``_GENERIC_5XX_MESSAGE``: leaking
    # ``ValueError`` / ``IntegrityError`` / ``KeyError`` class names
    # to public callers gives away implementation detail. 4xx uses
    # ``request_error`` as the catch-all when the exception class
    # doesn't define a more specific type.
    _GENERIC_5XX_TYPE = "internal_error"
    _GENERIC_4XX_TYPE = "request_error"

    def format_error(self, exception: Exception, status_code: int) -> dict[str, Any]:
        """Format general errors.

        ROOT-CAUSE (stress test scenario 4 / cycle 1): debug-mode
        404 / 422 / 401 / 403 responses were shipping ``file`` /
        ``line`` / full Python ``trace`` arrays in the JSON body. A
        ``GET /api/items/<bad-slug>`` 404 returned an 8.6 KB
        envelope with ``app/services/ExampleService.py:295``
        and the entire framework call stack pasted in. Even with
        ``app.debug=True``, 4xx responses are EXPECTED application
        behaviour (validation failed / not found / forbidden) — the
        caller acted on a bad input, the server didn't fault. Stack
        traces / file paths are diagnostics for unexpected 5xx faults
        only; surfacing them on 4xx leaks repository structure to
        anyone who can hit the API and bloats every "this slug
        doesn't exist" response.

        New rule: ``type`` (the exception class name) stays for
        debug-mode 4xx as a useful tag for the client's error UX,
        but ``file`` / ``line`` / ``trace`` are reserved for the 5xx
        path. Production behaviour is unchanged.
        """
        debug = self.is_debug_mode()

        # In production, redact the raw exception message for any unexpected
        # 5xx — `str(exception)` can carry SQL fragments, library internals,
        # or filesystem paths. 4xx messages are intentional (validation /
        # not-found / forbidden) and stay verbatim so callers can act.
        if status_code >= 500 and not debug:
            response: dict[str, Any] = {"error": self._GENERIC_5XX_MESSAGE}
        else:
            response = {"error": str(exception)}

        # Always include a machine-readable ``type``. Pre-fix the
        # generic-error path emitted ``{error: "..."}`` only — client
        # / SDK code had to substring-match the human message to branch
        # on error class. ``type`` is now part of the response contract
        # everywhere, with 5xx-in-prod collapsed to ``internal_error``
        # so we don't leak the actual exception class to public callers
        # (mirrors the ``_GENERIC_5XX_MESSAGE`` redaction policy).
        if status_code >= 500 and not debug:
            response["type"] = self._GENERIC_5XX_TYPE
        elif debug:
            # Debug + 5xx OR debug + 4xx: emit the raw class name as the
            # ``type``. Useful tag for the client's error UX during
            # development; matches the existing debug-mode behaviour.
            response["type"] = exception.__class__.__name__
        else:
            # 4xx in production. Don't leak the class name — emit the
            # generic 4xx token. Typed exceptions like
            # ``AuthorizationException`` define their own ``to_dict``
            # and never reach this branch, so they keep their specific
            # token ("authorization_error", etc.).
            response["type"] = self._GENERIC_4XX_TYPE

        if debug and status_code >= 500:
            # Only attach diagnostic stack/file/line for genuine 5xx
            # faults. 4xx responses are documented application
            # outcomes and should stay clean even when debug is on.
            response.update(
                {
                    "file": self.get_exception_file(exception),
                    "line": self.get_exception_line(exception),
                    "trace": self.get_trace(exception),
                }
            )

        return response

    def log_exception(self, exception: Exception) -> None:
        """Log the exception with structured context.

        4xx errors are expected application behaviour (not-found,
        validation, auth) so they log at WARNING. 5xx errors are
        genuine server faults and log at ERROR with a full traceback.
        """
        try:
            facades = importlib.import_module("cara.facades")

            status = self.get_status_code(exception)
            exc_type = exception.__class__.__name__
            if status < 500:
                facades.Log.warning(
                    "%s: %s",
                    exc_type,
                    exception,
                    category="cara.exceptions",
                    context={
                        "status_code": status,
                        "exception_type": exc_type,
                    },
                )
            else:
                facades.Log.error(
                    "%s: %s",
                    exc_type,
                    exception,
                    category="cara.exceptions",
                    exc_info=True,
                    context={
                        "status_code": status,
                        "exception_type": exc_type,
                    },
                )
                # This handler CONSUMES the exception (the response is built
                # here, nothing re-raises), so no Sentry integration hook
                # ever sees a 500 — the explicit capture is the only path.
                observability = importlib.import_module("cara.observability")
                observability.capture_exception(exception)
        except ImportError:
            pass

    def _cors_headers_for_scope(self, scope: dict[str, Any]) -> list:
        return _EXCEPTION_RESPONSE_HEADERS.cors(
            scope,
            on_policy_unavailable=self._log_cors_policy_unavailable,
        )

    @staticmethod
    def _log_cors_policy_unavailable() -> None:
        """Make an unreadable CORS policy observable rather than silent.

        Failing closed without a signal turns a configuration outage into
        "the dashboard mysteriously reports CORS errors on every 500".
        The logging facade may itself be unavailable this early, so its
        absence must not replace the original failure.
        """
        try:
            facades = importlib.import_module("cara.facades")

            facades.Log.warning(
                "CORS policy unreadable on the error path — emitting no CORS headers",
                category="cara.exceptions",
                exc_info=True,
            )
        except ImportError, RuntimeError:
            pass

    def _security_headers_for_scope(self, scope: dict[str, Any]) -> list:
        return _EXCEPTION_RESPONSE_HEADERS.security(scope)

    @staticmethod
    def _is_https_for_scope(scope: Any) -> bool:
        return _EXCEPTION_RESPONSE_HEADERS.is_https(scope)

    def _request_id_header_for(self, request: Any, scope: dict[str, Any]) -> list:
        return _EXCEPTION_RESPONSE_HEADERS.request_id(request, scope)

    @staticmethod
    def _retry_after_header_for(data: dict[str, Any]) -> list:
        return _EXCEPTION_RESPONSE_HEADERS.retry_after(data)

    @staticmethod
    def _allow_header_for(data: dict[str, Any]) -> list:
        return _EXCEPTION_RESPONSE_HEADERS.allow(data)

    async def send_response(
        self,
        data: dict[str, Any],
        status_code: int,
        scope: dict[str, Any],
        receive: Any,
        send: Any,
        request: Any = None,
    ) -> None:
        """Send the response."""
        if scope.get("response_started") and not scope.get("response_sent"):
            # A response START is already on the wire (the body send then
            # failed): the status cannot change and any new
            # ``http.response.start`` is an ASGI protocol violation that
            # cascades — uvicorn rejects it, the rejection lands back
            # here, and every fallback layer repeats the offence. Close
            # the body and let the logged exception carry the diagnosis.
            # Guarded BEFORE header assembly: none of those headers can
            # be sent on a started connection anyway.
            with contextlib.suppress(Exception):
                await send(
                    {"type": "http.response.body", "body": b"", "more_body": False}
                )
            scope["response_sent"] = True
            return
        cors = self._cors_headers_for_scope(scope)
        sec = self._security_headers_for_scope(scope)
        rid = self._request_id_header_for(request, scope)
        # ``Allow`` HTTP header for 405 Method Not Allowed responses
        # (RFC 9110 §15.5.6). The router stamps the allow-list on
        # ``MethodNotAllowedException`` via the ``allowed`` kwarg;
        # we surface it here so the response carries the per-route
        # allow list rather than the generic CORS-allowed-methods
        # config which describes the cross-origin policy, not the
        # resource's supported verbs. ``_allow_header_for`` is a
        # no-op when the exception doesn't carry the attribute, so
        # non-405 responses get an empty list and add nothing.
        allow = self._allow_header_for(data)
        # ``Retry-After`` HTTP header. ``HttpException.ServiceUnavailable
        # Exception`` documents the contract: ``retry_after`` is surfaced
        # both in the JSON envelope AND the ``Retry-After`` header so
        # callers don't have to parse the body to know when to come back.
        # Pre-fix only the body half landed — ``format_response`` (above)
        # propagated ``retry_after`` into ``data``, but ``send_response``
        # only emitted CORS / security / request-id headers and never
        # promoted it to the HTTP layer. Load balancers, browser retry,
        # urllib3 ``Retry`` adapters, ``requests``' ``Retry`` config —
        # all look at the header, not the body — fall back to default
        # exponential backoff or a long pin. ``DatabaseUnavailableException``
        # (raised on pool exhaustion) was the load-bearing case: clients
        # got ``retry_after=1`` in the JSON but the HTTP header was
        # absent, so they kept retrying every 30+ seconds. Pull from
        # ``data`` since ``format_response`` is the single source of
        # truth (works for both ``to_dict``-defining exceptions and the
        # ``getattr(exception, "retry_after", ...)`` fallback path).
        retry = self._retry_after_header_for(data)
        extras = cors + sec + rid + retry + allow
        try:
            if self.application:
                response = self.application.make("response")
                response.json(data, status=status_code)
                for key, val in extras:
                    response.header(key.decode(), val.decode())
                if not scope.get("response_sent") and not response.is_sent():
                    await response(scope, receive, send)
            else:
                await self.send_manual_response(
                    data, status_code, scope, receive, send, extras
                )
        except Exception:
            await self.send_manual_response(
                data, status_code, scope, receive, send, extras
            )

    async def send_manual_response(
        self,
        data: dict[str, Any],
        status_code: int,
        scope: dict[str, Any],
        receive: Any,
        send: Any,
        extra_headers: list | None = None,
    ) -> None:
        """Manual response fallback."""
        if scope.get("response_started") and not scope.get("response_sent"):
            # Same guard as ``send_response`` — this path is also reached
            # DIRECTLY from its except clause, after the shared response
            # object failed mid-send.
            with contextlib.suppress(Exception):
                await send(
                    {"type": "http.response.body", "body": b"", "more_body": False}
                )
            scope["response_sent"] = True
            return
        response_body = json.dumps(data).encode("utf-8")

        # Match the success-path content-type (includes charset) so a
        # client picking the type up programmatically sees the same
        # value on success and error responses. Scenario 7 / cycle 1.
        headers = [
            [b"content-type", b"application/json; charset=utf-8"],
            [b"content-length", str(len(response_body)).encode()],
        ]
        if extra_headers:
            headers.extend(extra_headers)

        await send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": headers,
            }
        )

        await send(
            {
                "type": "http.response.body",
                "body": response_body,
            }
        )

    def is_debug_mode(self) -> bool:
        """Check if in debug mode."""
        try:
            from cara.configuration import (  # local: cycle with cara.configuration
                config,
            )

            return config("app.debug", False)
        except ImportError, RuntimeError, TypeError:
            return False

    def get_exception_file(self, exception: Exception) -> str | None:
        """Get file where exception occurred."""
        try:
            tb = exception.__traceback__
            if tb:
                while tb.tb_next:
                    tb = tb.tb_next
                return tb.tb_frame.f_code.co_filename
        except OSError, RuntimeError, AttributeError, ConnectionError:
            pass
        return None

    def get_exception_line(self, exception: Exception) -> int | None:
        """Get line where exception occurred."""
        try:
            tb = exception.__traceback__
            if tb:
                while tb.tb_next:
                    tb = tb.tb_next
                return tb.tb_lineno
        except OSError, RuntimeError, AttributeError, ConnectionError:
            pass
        return None

    def get_trace(self, exception: Exception) -> list:
        """Get formatted traceback."""
        try:
            return traceback.format_exc().split("\n")
        except RuntimeError, TypeError, ValueError:
            return []
