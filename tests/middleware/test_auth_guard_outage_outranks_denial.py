"""With several guards, an outage in one is never reported as a 401.

``AuthenticateUser`` classified a guard's exception with
``is_http_exception``; ``DatabaseUnavailableException`` sets it, so a 503
from the first guard (auth backend down) was remembered as a denial and then
overwritten by the second guard's 401: the client was told "bad
credentials". Both middlewares now share ``ShouldAuthenticate.guard_denial``.
"""

from __future__ import annotations

import asyncio
import importlib
from unittest.mock import MagicMock

import pytest

from cara.exceptions import DatabaseUnavailableException, InvalidTokenException
from cara.middleware.http.AuthenticateUser import AuthenticateUser
from cara.middleware.http.ShouldAuthenticate import ShouldAuthenticate

# The module, not the class the package re-exports under the same name.
should_module = importlib.import_module("cara.middleware.http.ShouldAuthenticate")


class _Guard:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def user(self):
        raise self._error


class _AuthManager:
    def __init__(self, errors: dict[str, Exception]) -> None:
        self._errors = errors

    def guard(self, name: str) -> _Guard:
        return _Guard(self._errors[name])


class _Application:
    def __init__(self, errors: dict[str, Exception]) -> None:
        self._manager = _AuthManager(errors)

    def make(self, binding: str) -> _AuthManager:
        assert binding == "auth"
        return self._manager


class _Captured:
    def __init__(self, _application) -> None:
        self.body: dict | None = None
        self.status: int | None = None

    def json(self, body: dict, status: int = 200):
        self.body, self.status = body, status
        return self


@pytest.fixture(autouse=True)
def captured_response(monkeypatch):
    monkeypatch.setattr(should_module, "Response", _Captured)


def _outage() -> DatabaseUnavailableException:
    return DatabaseUnavailableException("auth backend down", retry_after=1)


def _denied() -> InvalidTokenException:
    return InvalidTokenException("Token has expired")


async def _next(_request):  # pragma: no cover - never reached
    raise AssertionError("the pipeline must not continue past a failed guard")


def _run(cls, errors: dict[str, Exception]):
    middleware = cls.__new__(cls)
    middleware.application = _Application(errors)
    middleware.guards = list(errors)
    return asyncio.run(middleware.handle(MagicMock(), _next))


MIDDLEWARES = [AuthenticateUser, ShouldAuthenticate]


@pytest.mark.parametrize("cls", MIDDLEWARES)
@pytest.mark.parametrize(
    "errors",
    [
        {"jwt": _outage(), "api_key": _denied()},  # outage first
        {"jwt": _denied(), "api_key": _outage()},  # outage last
    ],
    ids=["outage-then-401", "401-then-outage"],
)
def test_an_outage_from_any_guard_outranks_a_401(cls, errors):
    with pytest.raises(DatabaseUnavailableException):
        _run(cls, errors)


@pytest.mark.parametrize("cls", MIDDLEWARES)
def test_a_single_failing_guard_is_unchanged(cls):
    with pytest.raises(DatabaseUnavailableException):
        _run(cls, {"jwt": _outage()})

    response = _run(cls, {"jwt": _denied()})
    assert response.status == 401


@pytest.mark.parametrize("cls", MIDDLEWARES)
def test_two_denials_still_answer_401(cls):
    response = _run(cls, {"jwt": _denied(), "api_key": _denied()})

    assert response.status == 401
