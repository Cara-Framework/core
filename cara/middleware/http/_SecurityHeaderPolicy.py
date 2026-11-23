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
    # ``SecurityHeaders`` is built per request: read the ``SECURITY`` dict
    # once and take the three keys from it. A reader that does not return the
    # dict (the all-defaults fallback, a test double keyed by full paths) is
    # asked key by key, as before.
    section = config_reader("security.security")
    if isinstance(section, dict):

        def read(name: str, default: Any = None) -> Any:
            return section.get(name, default)

    else:

        def read(name: str, default: Any = None) -> Any:
            return config_reader(f"security.security.{name}", default)

    headers = dict(_DEFAULT_HEADERS)
    overrides = read("headers")
    if isinstance(overrides, dict):
        for key, value in overrides.items():
            if value is None:
                headers.pop(key, None)
            else:
                headers[key] = str(value)

    unset = object()
    custom_hsts = read("hsts", unset)
    hsts: str | None = _DEFAULT_HSTS
    if custom_hsts is None:
        hsts = None
    elif custom_hsts is not unset and isinstance(custom_hsts, str):
        hsts = custom_hsts
    preload = bool(read("hsts_preload", False))
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

    forwarded_proto = _scope_header(scope, "x-forwarded-proto")
    if (
        isinstance(forwarded_proto, str)
        and forwarded_proto.split(",")[0].strip().lower() == "https"
    ):
        return True
    forwarded = _scope_header(scope, "forwarded")
    return bool(isinstance(forwarded, str) and "proto=https" in forwarded.lower())


def _scope_header(scope: dict, name: str) -> str | None:
    """One request header's value (the last occurrence), read as latin-1.

    Only the header asked for is decoded. This used to decode EVERY header as
    strict UTF-8 to read two of them, so behind a trusted proxy — any loopback
    peer counts — a single high byte anywhere (a latin-1 ``User-Agent``)
    raised ``UnicodeDecodeError``. On the success path ``SecurityHeaders``
    swallowed it and dropped HSTS; on the error path nothing caught it, the
    exception handler itself crashed, and a 404 or 401 left as a bare 500
    with no envelope and no CORS headers.
    """
    wanted = name.encode("ascii")
    found: str | None = None
    for key, value in scope.get("headers", []) or []:
        if isinstance(key, (bytes, bytearray)):
            if key.lower() != wanted:
                continue
        elif str(key).lower() != name:
            continue
        found = (
            value.decode("latin-1")
            if isinstance(value, (bytes, bytearray))
            else str(value)
        )
    return found
