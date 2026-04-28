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

from typing import Callable, Dict, Optional

from cara.configuration import config
from cara.http import Request
from cara.middleware import Middleware


_DEFAULT_HEADERS: Dict[str, str] = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    # Disable features the JSON API has no reason to use; reduces the
    # blast radius if an attacker ever tricks a browser into rendering a
    # response as HTML.
    "Permissions-Policy": (
        "accelerometer=(), autoplay=(), camera=(), geolocation=(), "
        "gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()"
    ),
    "Cross-Origin-Opener-Policy": "same-origin",
    "Cross-Origin-Resource-Policy": "same-site",
    # X-XSS-Protection is legacy; explicit "0" tells old browsers to use
    # the default CSP-based protections instead of their heuristic filter.
    "X-XSS-Protection": "0",
}

# HSTS — only added when request is HTTPS. 6 months + includeSubDomains.
_DEFAULT_HSTS = "max-age=15552000; includeSubDomains"


class SecurityHeaders(Middleware):
    """Attach baseline security headers to every response."""

    def __init__(self, application, parameters=None):
        super().__init__(application)
        self.parameters = parameters or []
        self._headers, self._hsts, self._hsts_preload = self._load_config()

    def _load_config(self):
        headers = dict(_DEFAULT_HEADERS)
        hsts: Optional[str] = _DEFAULT_HSTS
        preload = False

        try:
            overrides = config("security.security.headers")
            if isinstance(overrides, dict):
                for k, v in overrides.items():
                    if v is None:
                        headers.pop(k, None)
                    else:
                        headers[k] = str(v)

            custom_hsts = config("security.security.hsts")
            if custom_hsts is None:
                hsts = None
            elif isinstance(custom_hsts, str):
                hsts = custom_hsts

            preload = bool(config("security.security.hsts_preload", False))
        except Exception as e:
            self._log_debug(
                f"SecurityHeaders: failed to load config "
                f"({e.__class__.__name__}: {e})"
            )

        return headers, hsts, preload

    async def handle(self, request: Request, next: Callable):
        response = await next(request)

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
                f"SecurityHeaders: failed to attach headers "
                f"({e.__class__.__name__}: {e})"
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
            scheme = scope.get("scheme")
            if isinstance(scheme, str) and scheme.lower() == "https":
                return True

            if self._peer_is_trusted_proxy(scope):
                forwarded_proto = request.header("X-Forwarded-Proto")
                if (
                    isinstance(forwarded_proto, str)
                    and forwarded_proto.split(",")[0].strip().lower() == "https"
                ):
                    return True
                forwarded = request.header("Forwarded")
                if isinstance(forwarded, str) and "proto=https" in forwarded.lower():
                    return True
        except Exception as e:
            self._log_debug(
                f"SecurityHeaders: HTTPS detection raised "
                f"({e.__class__.__name__}: {e})"
            )
        return False

    def _peer_is_trusted_proxy(self, scope: dict) -> bool:
        """Decide whether to honour proxy-supplied scheme headers.

        Reads ``trustedproxies.proxies`` (list of CIDR strings) from
        config. Empty list → trust nobody (forwarded-proto ignored,
        only direct https counts). The "*" sentinel preserves the
        previous unconditional-trust behaviour for callers behind a
        single load balancer they fully control.
        """
        try:
            client = scope.get("client") or ()
            client_ip = client[0] if client else None
            if not client_ip:
                return False
            proxies = config("trustedproxies.proxies", config("security.security.trusted_proxies", []))
            if not proxies:
                return False
            if "*" in proxies:
                return True
            import ipaddress

            ip = ipaddress.ip_address(client_ip)
            for entry in proxies:
                try:
                    if ip in ipaddress.ip_network(entry, strict=False):
                        return True
                except ValueError:
                    continue
        except Exception:
            return False
        return False

    @staticmethod
    def _log_debug(msg: str) -> None:
        """Best-effort debug log; survives partial-boot when Log facade is missing."""
        try:
            from cara.facades import Log
            Log.debug(msg, category="cara.http.security_headers")
        except Exception as e:
            # Don't recurse into Log if Log itself failed; emit a
            # last-resort stderr line so the issue isn't fully invisible.
            import sys
            print(
                f"SecurityHeaders: log facade unavailable ({e.__class__.__name__}: {e}); "
                f"original msg: {msg}",
                file=sys.stderr,
            )
