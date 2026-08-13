"""
SecurityHeaders middleware — sets baseline defense-in-depth headers on every
HTTP response.

These are cheap wins. Most are ignored by non-browser clients but matter a
lot when a browser consumes the response or accidentally lands on an API URL.

Configurable via config/security.py → `SECURITY_HEADERS` dict, but defaults
are production-safe for a JSON API:

  - X-Content-Type-Options: nosniff
      Prevents browsers from MIME-sniffing a text response as HTML/JS.
  - X-Frame-Options: DENY
      Stops the API being iframed (clickjacking defense for any HTML leaks).
  - Referrer-Policy: strict-origin-when-cross-origin
      Doesn't leak path/query when navigating away to other origins.
  - Permissions-Policy
      Explicitly disables powerful browser features the API will never need.
  - Cross-Origin-Opener-Policy: same-origin
  - Cross-Origin-Resource-Policy: same-site
  - Strict-Transport-Security
      Enabled only when the request arrived over HTTPS (so local HTTP dev
      keeps working). Opt-out via config if you deliberately serve HTTP.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from cara.facades import Log
from cara.http import Request, Response
from cara.security import peer_is_trusted_proxy

from ..Middleware import Middleware
from ._SecurityHeaderPolicy import (
    _load_security_header_policy,
    _scope_is_https,
)


class SecurityHeaders(Middleware):
    """Attach baseline security headers to every response."""

    def __init__(self, application, parameters=None):
        super().__init__(application)
        self.parameters = parameters or []
        self._headers, self._hsts, self._hsts_preload = self._load_config()

    def _load_config(self):
        try:
            return _load_security_header_policy()
        except Exception as e:
            self._log_debug(
                f"SecurityHeaders: failed to load config ({e.__class__.__name__}: {e})"
            )
            return _load_security_header_policy(lambda _key, default=None: default)

    async def handle(
        self, request: Request, next_fn: Callable[..., Awaitable[Any]]
    ) -> Response:
        response = await next_fn(request)

        try:
            for name, value in self._headers.items():
                response.header(name, value)

            if self._hsts and self._is_https(request):
                value = self._hsts
                if self._hsts_preload and "preload" not in value:
                    value = f"{value}; preload"
                response.header("Strict-Transport-Security", value)
        except Exception as e:
            # Never break a response because a header couldn't be set —
            # the response itself is still what the caller needs. Log
            # at debug level instead of swallowing silently so a
            # systematic header-setting bug becomes visible during
            # incident review.
            self._log_debug(
                f"SecurityHeaders: failed to attach headers ({e.__class__.__name__}: {e})"
            )

        return response

    def _is_https(self, request: Request) -> bool:
        """Detect HTTPS — ASGI scope first, X-Forwarded-Proto only when
        the request actually came from a trusted proxy.

        ``getattr(request, "scheme", None)`` returns ``None`` (Request
        has no ``scheme`` attribute — only ``self.scope["scheme"]``),
        so the previous check was a no-op and HSTS was never set when
        uvicorn terminated TLS itself. We now read the ASGI scope
        directly. ``X-Forwarded-Proto`` is honoured only when the
        immediate peer is a configured trusted proxy, otherwise any
        client could spoof it.
        """
        try:
            scope = getattr(request, "scope", None) or {}
            return _scope_is_https(scope)
        except Exception as e:
            self._log_debug(
                f"SecurityHeaders: HTTPS detection raised ({e.__class__.__name__}: {e})"
            )
        return False

    def _peer_is_trusted_proxy(self, scope: dict) -> bool:
        """Decide whether to honour proxy-supplied scheme headers.

        Delegates to :mod:`cara.security.TrustedProxies`, the single source for
        this boundary. This used to read ``trustedproxies.proxies`` falling
        back to ``security.security.trusted_proxies`` — two keys NO product
        defines, while both set ``app.trusted_proxies``. It therefore resolved
        to ``[]`` on every request, so HSTS was never emitted behind a
        TLS-terminating proxy. Do not reintroduce a local lookup here.
        """
        return peer_is_trusted_proxy(scope)

    @staticmethod
    def _log_debug(msg: str) -> None:
        """Debug log for a swallowed header-application failure.

        The old body claimed to "survive partial-boot when Log facade is
        missing" while its ``except`` re-imported and re-called that exact
        facade — if the import really had failed, the handler raised too and
        the logging helper became the cause of the outage it was meant to
        report. The missing-facade case has exactly one owner:
        ``Facade.__getattr__``'s ``cls.key == "logger"`` stdlib fallback.
        """
        Log.debug(msg, category="cara.http.security_headers")
