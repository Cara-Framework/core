"""HTTP header policy for framework exception responses."""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from typing import Any

_HTTP_METHOD = re.compile(r"^[A-Z][A-Z0-9_-]*$")


class _ExceptionResponseHeaders:
    """Build headers skipped when an exception unwinds HTTP middleware."""

    @staticmethod
    def cors(
        scope: dict[str, Any],
        *,
        on_policy_unavailable: Callable[[], None],
    ) -> list:
        import cara.middleware.http.Cors as CorsPolicy  # local: cycle with cara.middleware.http.Cors

        try:
            policy = CorsPolicy.load_cors_policy()
        except Exception:
            on_policy_unavailable()
            return []

        if not CorsPolicy.path_in_cors_scope(
            scope.get("path", ""),
            policy.get("paths"),
        ):
            return []

        raw_headers = dict(scope.get("headers", []))
        origin = raw_headers.get(b"origin", b"").decode()
        allow_origin = CorsPolicy.resolve_allow_origin(origin, policy)
        headers: list = []
        if allow_origin is not None:
            headers.append([b"access-control-allow-origin", allow_origin.encode()])
            if allow_origin != "*":
                headers.append([b"vary", b"Origin"])

        allowed_methods = policy.get("allowed_methods")
        allowed_headers = policy.get("allowed_headers")
        max_age = policy.get("max_age")
        if allowed_methods:
            headers.append(
                [b"access-control-allow-methods", ", ".join(allowed_methods).encode()]
            )
        if allowed_headers:
            headers.append(
                [b"access-control-allow-headers", ", ".join(allowed_headers).encode()]
            )
        if policy.get("supports_credentials"):
            headers.append([b"access-control-allow-credentials", b"true"])
        if max_age:
            headers.append([b"access-control-max-age", str(max_age).encode()])
        return headers

    @classmethod
    def security(cls, scope: dict[str, Any]) -> list:
        from cara.middleware.http._SecurityHeaderPolicy import (  # local: cycle with cara.middleware.http._SecurityHeaderPolicy
            _load_security_header_policy,
        )

        try:
            headers, hsts, hsts_preload = _load_security_header_policy()
        except OSError, RuntimeError, AttributeError, ConnectionError:
            headers, hsts, hsts_preload = _load_security_header_policy(
                lambda _key, default=None: default
            )

        out: list = [
            [key.lower().encode(), str(value).encode()] for key, value in headers.items()
        ]
        try:
            if hsts and cls.is_https(scope):
                value = hsts
                if hsts_preload and "preload" not in value:
                    value = f"{value}; preload"
                out.append([b"strict-transport-security", value.encode()])
        except OSError, RuntimeError, AttributeError, ConnectionError:
            pass
        return out

    @staticmethod
    def is_https(scope: Any) -> bool:
        from cara.middleware.http._SecurityHeaderPolicy import (  # local: cycle with cara.middleware.http._SecurityHeaderPolicy
            _scope_is_https,
        )

        return _scope_is_https(scope)

    @staticmethod
    def request_id(request: Any, scope: dict[str, Any]) -> list:
        try:
            request_id = (
                getattr(request, "request_id", None) if request is not None else None
            )
        except Exception:
            request_id = None
        if not request_id:
            try:
                raw = dict(scope.get("headers", []) if isinstance(scope, dict) else [])
                request_id = raw.get(b"x-request-id", b"").decode() or None
            except Exception:
                request_id = None
        if not request_id:
            request_id = str(uuid.uuid4())
        return [[b"x-request-id", request_id.encode()]]

    @staticmethod
    def retry_after(data: dict[str, Any]) -> list:
        raw = data.get("retry_after") if isinstance(data, dict) else None
        if raw is None:
            return []
        try:
            seconds = int(raw)
        except TypeError, ValueError:
            return []
        if seconds <= 0:
            return []
        return [[b"retry-after", str(seconds).encode()]]

    @staticmethod
    def allow(data: dict[str, Any]) -> list:
        raw = data.get("allowed") if isinstance(data, dict) else None
        if not raw:
            return []
        try:
            methods = [
                str(method).upper().strip() for method in raw if str(method).strip()
            ]
        except TypeError:
            return []
        safe = [method for method in methods if _HTTP_METHOD.fullmatch(method)]
        if not safe:
            return []
        return [[b"allow", ", ".join(safe).encode()]]


_EXCEPTION_RESPONSE_HEADERS = _ExceptionResponseHeaders()
