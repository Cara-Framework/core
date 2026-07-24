"""Authentication middleware must use explicit guard configuration."""

from __future__ import annotations

import pytest

from cara.middleware.http.AuthenticateUser import AuthenticateUser
from cara.middleware.http.ShouldAuthenticate import ShouldAuthenticate


class _AuthManager:
    def __init__(self, default: str = "api_key") -> None:
        self.default = default

    def get_default_guard(self) -> str:
        return self.default


class _Application:
    def __init__(self, auth_manager=None, error: Exception | None = None) -> None:
        self.auth_manager = auth_manager
        self.error = error
        self.calls = 0

    def make(self, binding: str):
        self.calls += 1
        assert binding == "auth"
        if self.error is not None:
            raise self.error
        return self.auth_manager


def test_default_guard_resolution_failure_is_not_replaced_with_jwt() -> None:
    application = _Application(error=RuntimeError("auth binding missing"))

    with pytest.raises(RuntimeError, match="auth binding missing"):
        ShouldAuthenticate(application)


def test_configured_default_guard_is_used_verbatim() -> None:
    middleware = ShouldAuthenticate(_Application(_AuthManager("api_key")))

    assert middleware.guards == ["api_key"]


def test_explicit_guards_do_not_resolve_or_rewrite_the_default() -> None:
    application = _Application(error=AssertionError("must not resolve auth"))
    middleware = AuthenticateUser(application, guards=["jwt"])

    assert middleware._resolve_guards() == ["jwt"]
    assert application.calls == 0
