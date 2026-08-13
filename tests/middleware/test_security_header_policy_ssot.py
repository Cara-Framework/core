"""Success and exception paths consume one security-header policy."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import cara.middleware.http._SecurityHeaderPolicy as policy
from cara.exceptions.handlers.DefaultExceptionHandler import DefaultExceptionHandler
from cara.middleware.http.SecurityHeaders import SecurityHeaders


def _config(values):
    return lambda key, default=None: values.get(key, default)


def _error_headers(scope: dict) -> dict[bytes, bytes]:
    return dict(DefaultExceptionHandler()._security_headers_for_scope(scope))


def test_absent_hsts_config_keeps_the_safe_default_on_both_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(policy.configuration, "config", _config({}))
    scope = {"scheme": "https", "headers": [], "client": None}
    middleware = SecurityHeaders(None)

    assert middleware._hsts == "max-age=15552000; includeSubDomains"
    assert _error_headers(scope)[b"strict-transport-security"] == (
        b"max-age=15552000; includeSubDomains"
    )


def test_explicit_hsts_opt_out_applies_to_both_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        policy.configuration,
        "config",
        _config({"security.security.hsts": None}),
    )
    scope = {"scheme": "https", "headers": [], "client": None}
    middleware = SecurityHeaders(None)

    assert middleware._hsts is None
    assert b"strict-transport-security" not in _error_headers(scope)


def test_forwarded_https_requires_the_same_trusted_peer_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(policy.configuration, "config", _config({}))
    scope = {
        "scheme": "http",
        "headers": [(b"x-forwarded-proto", b"https")],
        "client": ("203.0.113.8", 1234),
    }
    request = SimpleNamespace(scope=scope)
    middleware = SecurityHeaders(None)

    monkeypatch.setattr(policy, "peer_is_trusted_proxy", lambda _scope: False)
    assert middleware._is_https(request) is False
    assert b"strict-transport-security" not in _error_headers(scope)

    monkeypatch.setattr(policy, "peer_is_trusted_proxy", lambda _scope: True)
    assert middleware._is_https(request) is True
    assert b"strict-transport-security" in _error_headers(scope)
