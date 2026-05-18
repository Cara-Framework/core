"""
Default Exception Handler.

Professional exception handler using proper exception hierarchy.
"""

import traceback
from typing import Any


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

    def format_response(self, exception: Exception, status_code: int) -> dict[str, Any]:
        """Format the exception into a response."""
        # If exception has its own to_dict method, use it
        if hasattr(exception, "to_dict") and callable(exception.to_dict):
            return exception.to_dict()

        # Default formatting for exceptions without to_dict
        return self.format_error(exception, status_code)

    # Generic message for unexpected 5xx errors when not in debug. The real
    # exception still hits the logs (with exc_info) — we just don't ship
    # internals (SQL errors, file paths, lib stack frames) to the caller.
    _GENERIC_5XX_MESSAGE = "Internal server error"

    # Machine-readable ``type`` tokens for the generic-error path
    # (exceptions that don't define ``to_dict``). The contract: every
    # error response carries a stable ``type`` string the client can
    # branch on, so storefront / SDK code doesn't have to substring-
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

        ROOT-CAUSE (frontend stress scenario 4 / cycle 1): debug-mode
        404 / 422 / 401 / 403 responses were shipping ``file`` /
        ``line`` / full Python ``trace`` arrays in the JSON body. A
        ``GET /api/products/<bad-slug>`` 404 returned an 8.6 KB
        envelope with ``app/services/ProductDetailService.py:295``
        and the entire framework call stack pasted in. Even with
        ``app.debug=True``, 4xx responses are EXPECTED application
        behaviour (validation failed / not found / forbidden) — the
        caller acted on a bad input, the server didn't fault. Stack
        traces / file paths are diagnostics for unexpected 5xx faults
        only; surfacing them on 4xx leaks repository structure to
        anyone who can hit the API and bloats every "this slug
        doesn't exist" response.

        New rule: ``type`` (the exception class name) stays for
        debug-mode 4xx as a useful tag for the storefront's error UX,
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
        # generic-error path emitted ``{error: "..."}`` only — storefront
        # / SDK code had to substring-match the human message to branch
        # on error class. ``type`` is now part of the response contract
        # everywhere, with 5xx-in-prod collapsed to ``internal_error``
        # so we don't leak the actual exception class to public callers
        # (mirrors the ``_GENERIC_5XX_MESSAGE`` redaction policy).
        if status_code >= 500 and not debug:
            response["type"] = self._GENERIC_5XX_TYPE
        elif debug:
            # Debug + 5xx OR debug + 4xx: emit the raw class name as the
            # ``type``. Useful tag for the storefront's error UX during
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
        """Log the exception.

        4xx errors are expected application behaviour (not-found,
        validation, auth) so they log at WARNING. 5xx errors are
        genuine server faults and log at ERROR with a full traceback.
        """
        try:
            from cara.facades import Log

            status = self.get_status_code(exception)
            msg = f"{exception.__class__.__name__}: {str(exception)}"
            if status < 500:
                Log.warning(msg, category="cara.exceptions")
            else:
                Log.error(msg, category="cara.exceptions", exc_info=True)
        except ImportError:
            pass

    def _cors_headers_for_scope(self, scope: dict[str, Any]) -> list:
        """Build CORS header pairs for an error response.

        Mirrors the credentials/wildcard guard in ``HandleCors``: when
        credentials are enabled we MUST NOT echo an arbitrary origin
        next to ``Access-Control-Allow-Credentials: true``. The fix in
        the live HandleCors path was useless if the exception path
        kept reflecting; both have to apply the same rule.
        """
        try:
            from cara.configuration import config

            allowed_origins = config("cors.cors.allowed_origins", ["*"])
            allowed_origins_patterns = config("cors.cors.allowed_origins_patterns", [])
            supports_credentials = config("cors.cors.supports_credentials", False)
            allowed_methods = config("cors.cors.allowed_methods", ["*"])
            allowed_headers = config("cors.cors.allowed_headers", ["*"])
            max_age = config("cors.cors.max_age", 0)
        except Exception:
            allowed_origins = ["*"]
            allowed_origins_patterns = []
            supports_credentials = False
            allowed_methods = ["*"]
            allowed_headers = ["*"]
            max_age = 0

        raw_headers = dict(scope.get("headers", []))
        origin = raw_headers.get(b"origin", b"").decode()

        def _explicit_match(o: str) -> bool:
            if not o:
                return False
            if o in allowed_origins:
                return True
            import re as _re

            for pat in allowed_origins_patterns or []:
                if _re.match(pat, o):
                    return True
            return False

        headers: list = []

        if supports_credentials:
            # Only echo when there's an explicit allowlist match;
            # never with a wildcard.
            if origin and _explicit_match(origin):
                headers.append([b"access-control-allow-origin", origin.encode()])
                headers.append([b"vary", b"Origin"])
        else:
            if "*" in allowed_origins:
                headers.append([b"access-control-allow-origin", b"*"])
            elif origin and _explicit_match(origin):
                headers.append([b"access-control-allow-origin", origin.encode()])
                headers.append([b"vary", b"Origin"])

        if allowed_methods:
            headers.append(
                [b"access-control-allow-methods", ", ".join(allowed_methods).encode()]
            )
        if allowed_headers:
            headers.append(
                [b"access-control-allow-headers", ", ".join(allowed_headers).encode()]
            )
        if supports_credentials:
            headers.append([b"access-control-allow-credentials", b"true"])
        if max_age:
            headers.append([b"access-control-max-age", str(max_age).encode()])

        return headers

    def _security_headers_for_scope(self, scope: dict[str, Any]) -> list:
        """Build defense-in-depth header pairs for an error response.

        ROOT-CAUSE (frontend stress scenario 7 / cycle 1): every error
        response (404 route-not-found, 405 method-not-allowed, 422
        validation, 401/403 auth, 5xx) bypassed the
        ``SecurityHeaders`` middleware because the exception path
        unwinds the middleware stack. The header sweep observed:

          * 200/204 success responses had nosniff / DENY / Permissions-
            Policy / CSP / COOP / CORP / Referrer-Policy / X-XSS-
            Protection / X-Permitted-Cross-Domain-Policies — every
            baseline header.
          * 404 / 405 / 422 / 4xx error responses had ZERO of those.
            ``curl /api/no-such-endpoint`` came back with only CORS
            and content-type. A browser landing on a 404 JSON URL
            would happily MIME-sniff it as HTML.

        Mirroring the SecurityHeaders middleware here means the
        baseline applies on every code path, not just the happy path.
        Config overrides (``security.security.headers``) honour the
        same dict and ``None`` values still suppress a header.
        """
        from cara.middleware.http.SecurityHeaders import (
            _DEFAULT_HEADERS as _SH_DEFAULT_HEADERS,
        )
        from cara.middleware.http.SecurityHeaders import (
            _DEFAULT_HSTS as _SH_DEFAULT_HSTS,
        )

        headers_dict: dict[str, str] = dict(_SH_DEFAULT_HEADERS)
        hsts: str | None = _SH_DEFAULT_HSTS
        hsts_preload = False
        try:
            from cara.configuration import config

            overrides = config("security.security.headers")
            if isinstance(overrides, dict):
                for k, v in overrides.items():
                    if v is None:
                        headers_dict.pop(k, None)
                    else:
                        headers_dict[k] = str(v)

            custom_hsts = config("security.security.hsts")
            if custom_hsts is None and "security.security.hsts" in (
                getattr(config, "loaded_keys", set()) or set()
            ):
                hsts = None
            elif isinstance(custom_hsts, str):
                hsts = custom_hsts
            hsts_preload = bool(config("security.security.hsts_preload", False))
        except Exception:
            pass

        out: list = [
            [k.lower().encode(), str(v).encode()] for k, v in headers_dict.items()
        ]

        # HSTS only on HTTPS — match SecurityHeaders middleware logic.
        try:
            scheme = scope.get("scheme") if isinstance(scope, dict) else None
            if hsts and isinstance(scheme, str) and scheme.lower() == "https":
                value = hsts
                if hsts_preload and "preload" not in value:
                    value = f"{value}; preload"
                out.append([b"strict-transport-security", value.encode()])
        except Exception:
            pass

        return out

    def _request_id_header_for(self, request: Any, scope: dict[str, Any]) -> list:
        """Return the X-Request-ID header pair for the error response.

        The ``AttachRequestID`` middleware sets this on success
        responses, but on the exception path the middleware never
        wraps a response, so the header is missing. Same scenario 7
        finding as the security headers above — without the request
        id, an ops engineer correlating a user complaint to a Sentry
        event has to fall back to timestamp matching.
        """
        rid: str | None = None
        try:
            rid = getattr(request, "request_id", None) if request is not None else None
        except Exception:
            rid = None
        if not rid:
            try:
                raw = dict(scope.get("headers", []) if isinstance(scope, dict) else [])
                rid = raw.get(b"x-request-id", b"").decode() or None
            except Exception:
                rid = None
        if not rid:
            try:
                import uuid

                rid = str(uuid.uuid4())
            except Exception:
                rid = ""
        return [[b"x-request-id", rid.encode()]] if rid else []

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
        cors = self._cors_headers_for_scope(scope)
        sec = self._security_headers_for_scope(scope)
        rid = self._request_id_header_for(request, scope)
        extras = cors + sec + rid
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
        import json

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
            from cara.configuration import config

            return config("app.debug", False)
        except Exception:
            return False

    def get_exception_file(self, exception: Exception) -> str | None:
        """Get file where exception occurred."""
        try:
            tb = exception.__traceback__
            if tb:
                while tb.tb_next:
                    tb = tb.tb_next
                return tb.tb_frame.f_code.co_filename
        except Exception:
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
        except Exception:
            pass
        return None

    def get_trace(self, exception: Exception) -> list:
        """Get formatted traceback."""
        try:
            return traceback.format_exc().split("\n")
        except Exception:
            return []
