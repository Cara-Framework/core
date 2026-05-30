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
            try:
                response["retry_after"] = int(retry_after)
            except (TypeError, ValueError):
                # Non-int values shouldn't reach here, but if they do
                # don't poison the response — drop silently.
                pass

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

            # HSTS opt-out parity with ``SecurityHeaders._load_config``.
            # Pre-fix this branch tried to distinguish "explicit None"
            # from "absent config" via ``getattr(config, "loaded_keys",
            # set())`` — but the ``config`` callable has no such
            # attribute, so the ``and`` half was always False and the
            # opt-out path NEVER fired. Result: a deployment that set
            # ``SECURITY_HSTS=`` (empty env → ``security.security.hsts
            # = None``) had the success-path middleware strip HSTS on
            # 200s while the error path silently kept stamping
            # ``_DEFAULT_HSTS`` on 4xx / 5xx. Mixed coverage caches the
            # error-path pin in the browser and confuses every later
            # debug session ("HSTS is set sometimes?").
            #
            # Use a real sentinel so "absent config" preserves the
            # baseline default (defence-in-depth) AND explicit None
            # strips (operator's documented opt-out path). Mirrors the
            # config-loader-shaped semantics across both code paths.
            _UNSET = object()
            custom_hsts = config("security.security.hsts", _UNSET)
            if custom_hsts is _UNSET:
                # Unconfigured — keep ``_SH_DEFAULT_HSTS`` already set above.
                pass
            elif custom_hsts is None:
                hsts = None  # explicit opt-out
            elif isinstance(custom_hsts, str):
                hsts = custom_hsts
            hsts_preload = bool(config("security.security.hsts_preload", False))
        except Exception:
            pass

        out: list = [
            [k.lower().encode(), str(v).encode()] for k, v in headers_dict.items()
        ]

        # HSTS only on HTTPS — must mirror ``SecurityHeaders._is_https``
        # exactly, otherwise success vs. error responses ship
        # inconsistent HSTS coverage. Real failure mode: a TLS-terminating
        # load balancer (ALB / Cloudflare / nginx) gives the worker
        # ``scope.scheme == "http"`` but ``X-Forwarded-Proto: https``.
        # The success path's middleware honours the forwarded proto
        # (when peer is in ``trustedproxies.proxies``), so it stamps HSTS
        # on the 200. The error path used to compare ``scope.scheme``
        # only — the matching 404 came back without HSTS. Mixed coverage
        # is worse than uniform absence: the browser cache keeps the
        # success-path pin while error responses look "downgraded",
        # making the asymmetry hard to spot.
        try:
            if hsts and self._is_https_for_scope(scope):
                value = hsts
                if hsts_preload and "preload" not in value:
                    value = f"{value}; preload"
                out.append([b"strict-transport-security", value.encode()])
        except Exception:
            pass

        return out

    @staticmethod
    def _is_https_for_scope(scope: Any) -> bool:
        """Mirror ``SecurityHeaders._is_https`` for the error-response path.

        Order of trust:
          1. ``scope["scheme"] == "https"`` (direct TLS at the worker).
          2. When the immediate peer is in ``trustedproxies.proxies``,
             honour ``X-Forwarded-Proto`` and RFC 7239 ``Forwarded``.

        Any signal from an untrusted peer is ignored so a public client
        cannot forge HSTS by setting ``X-Forwarded-Proto: https``.
        """
        if not isinstance(scope, dict):
            return False
        scheme = scope.get("scheme")
        if isinstance(scheme, str) and scheme.lower() == "https":
            return True

        try:
            from cara.configuration import config

            proxies = config(
                "trustedproxies.proxies",
                config("security.security.trusted_proxies", []),
            )
        except Exception:
            proxies = []
        if not proxies:
            return False

        client = scope.get("client") or ()
        client_ip = client[0] if client else None
        if not client_ip:
            return False

        trusted = False
        if "*" in proxies:
            trusted = True
        else:
            try:
                import ipaddress

                ip = ipaddress.ip_address(client_ip)
                for entry in proxies:
                    try:
                        if ip in ipaddress.ip_network(entry, strict=False):
                            trusted = True
                            break
                    except ValueError:
                        continue
            except Exception:
                trusted = False
        if not trusted:
            return False

        raw_headers = {
            (k.decode().lower() if isinstance(k, (bytes, bytearray)) else str(k).lower()): (
                v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
            )
            for k, v in scope.get("headers", []) or []
        }
        forwarded_proto = raw_headers.get("x-forwarded-proto")
        if (
            isinstance(forwarded_proto, str)
            and forwarded_proto.split(",")[0].strip().lower() == "https"
        ):
            return True
        forwarded = raw_headers.get("forwarded")
        if isinstance(forwarded, str) and "proto=https" in forwarded.lower():
            return True
        return False

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
