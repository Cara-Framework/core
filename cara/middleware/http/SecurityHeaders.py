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
            cfg = config("security", {}) or {}
        except Exception:
            cfg = {}

        # Allow merging / overriding individual headers.
        overrides = cfg.get("headers") if isinstance(cfg, dict) else None
        if isinstance(overrides, dict):
            # Value of None deletes the default header.
            for k, v in overrides.items():
                if v is None:
                    headers.pop(k, None)
                else:
                    headers[k] = str(v)

        if isinstance(cfg, dict):
            custom_hsts = cfg.get("hsts")
            if custom_hsts is None:
                hsts = None  # explicit opt-out
            elif isinstance(custom_hsts, str):
                hsts = custom_hsts
            preload = bool(cfg.get("hsts_preload", False))

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
        except Exception:
            # Never break a response because a header couldn't be set —
            # the response itself is still what the caller needs.
            pass

        return response

    def _is_https(self, request: Request) -> bool:
        """Detect HTTPS — checks scheme + common reverse-proxy headers."""
        try:
            scheme = getattr(request, "scheme", None)
            if isinstance(scheme, str) and scheme.lower() == "https":
                return True
            forwarded_proto = request.header("X-Forwarded-Proto")
            if isinstance(forwarded_proto, str) and forwarded_proto.split(",")[0].strip().lower() == "https":
                return True
            forwarded = request.header("Forwarded")
            if isinstance(forwarded, str) and "proto=https" in forwarded.lower():
                return True
        except Exception:
            pass
        return False
