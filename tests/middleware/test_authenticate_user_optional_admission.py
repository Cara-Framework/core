"""``AuthenticateUserOptional`` hands the resolved identity to ``admit``.

The guard resolves the user on a worker thread, so its verified claims live
in a copied context and were lost: the request carried no ``jwt_claims`` and
a product could not ask whether the token's session was still live. A
logged-out token therefore kept every personalized response until it
expired. These tests pin both halves: the claims survive the thread hop, and
a refused identity degrades to the guest path instead of failing.
"""

from __future__ import annotations

import asyncio
import contextvars
from typing import Any

from cara.middleware.http.AuthenticateUserOptional import AuthenticateUserOptional

_USER = object()
_CLAIMS = {"sub": "7", "sid": "s-1"}


_PAYLOAD: contextvars.ContextVar[Any] = contextvars.ContextVar("payload", default=None)


class _Guard:
    """Stores claims in a ContextVar, exactly like ``JWTGuard``."""

    @property
    def _last_payload(self) -> Any:
        return _PAYLOAD.get()

    def user(self):
        _PAYLOAD.set(dict(_CLAIMS))
        return _USER


class _AuthManager:
    def guard(self, _name: str) -> _Guard:
        return _Guard()


class _Request:
    def __init__(self) -> None:
        self.user_value: Any = None
        self.jwt_claims: Any = None
        self._route_auth_guard: Any = None

    def set_user(self, user: Any) -> None:
        self.user_value = user


def _middleware(cls: type[AuthenticateUserOptional]) -> AuthenticateUserOptional:
    middleware = cls.__new__(cls)
    middleware._auth_manager = _AuthManager()
    middleware._resolve_auth_manager = lambda: middleware._auth_manager
    middleware._resolve_guards = lambda: ["jwt"]
    return middleware


async def _next(request: _Request) -> str:
    return "next"


def test_admitted_identity_carries_its_verified_claims() -> None:
    request = _Request()
    result = asyncio.run(_middleware(AuthenticateUserOptional).handle(request, _next))

    assert result == "next"
    assert request.user_value is _USER
    assert request.jwt_claims == _CLAIMS
    assert request._route_auth_guard == "jwt"


def test_refused_identity_proceeds_as_guest() -> None:
    seen: list[dict[str, Any]] = []

    class _RefuseRevoked(AuthenticateUserOptional):
        async def admit(self, request, user, claims):
            seen.append(claims)
            return False

    request = _Request()
    result = asyncio.run(_middleware(_RefuseRevoked).handle(request, _next))

    assert result == "next"
    assert seen == [_CLAIMS]
    assert request.user_value is None
    assert request.jwt_claims is None
