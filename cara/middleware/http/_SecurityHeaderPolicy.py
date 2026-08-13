"""Single security-header policy for success and exception HTTP paths."""

from __future__ import annotations

from typing import Any

import cara.configuration as configuration
from cara.security import peer_is_trusted_proxy

_DEFAULT_HEADERS: dict[str, str] = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Permissions-Policy": (
        "accelerometer=(), autoplay=(), camera=(), geolocation=(), "
        "gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()"
    ),
    "Cross-Origin-Opener-Policy": "same-origin",
    "Cross-Origin-Resource-Policy": "same-site",
    "X-XSS-Protection": "0",
    "Content-Security-Policy": (
        "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
    ),
    "X-Permitted-Cross-Domain-Policies": "none",
}
_DEFAULT_HSTS = "max-age=15552000; includeSubDomains"


def _load_security_header_policy(
    config_reader=None,
) -> tuple[dict[str, str], str | None, bool]:
    """Resolve one immutable snapshot of the configured header policy."""
    config_reader = config_reader or configuration.config
    headers = dict(_DEFAULT_HEADERS)
    overrides = config_reader("security.security.headers")
    if isinstance(overrides, dict):
        for key, value in overrides.items():
            if value is None:
                headers.pop(key, None)
            else:
                headers[key] = str(value)

    unset = object()
    custom_hsts = config_reader("security.security.hsts", unset)
    hsts: str | None = _DEFAULT_HSTS
    if custom_hsts is None:
        hsts = None
    elif custom_hsts is not unset and isinstance(custom_hsts, str):
        hsts = custom_hsts
    preload = bool(config_reader("security.security.hsts_preload", False))
    return headers, hsts, preload


def _scope_is_https(scope: Any) -> bool:
    """Trust proxy scheme headers only when the immediate peer is trusted."""
    if not isinstance(scope, dict):
        return False
    scheme = scope.get("scheme")
    if isinstance(scheme, str) and scheme.lower() == "https":
        return True
    if not peer_is_trusted_proxy(scope):
        return False

    raw_headers = {
        (
            key.decode().lower()
            if isinstance(key, (bytes, bytearray))
            else str(key).lower()
        ): (value.decode() if isinstance(value, (bytes, bytearray)) else str(value))
        for key, value in scope.get("headers", []) or []
    }
    forwarded_proto = raw_headers.get("x-forwarded-proto")
    if (
        isinstance(forwarded_proto, str)
        and forwarded_proto.split(",")[0].strip().lower() == "https"
    ):
        return True
    forwarded = raw_headers.get("forwarded")
    return bool(isinstance(forwarded, str) and "proto=https" in forwarded.lower())
