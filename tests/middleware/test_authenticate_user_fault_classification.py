"""The redaction guarantee has to survive into the subclass that ships.

``tests/middleware/test_auth_failure_redaction`` drives ``ShouldAuthenticate``.
Nothing registers ``ShouldAuthenticate``. Both products alias ``auth`` to
``AuthenticateUser``, which OVERRIDES ``handle`` wholesale — its own guard
loop, its own ``run_in_thread`` offload, its own ``except`` clause — and that
clause still classifies with ``is_http_exception`` rather than with
``denial_status``, the predicate the base class introduced precisely because
``is_http_exception`` was not enough (``DatabaseUnavailableException`` sets it
and carries the psycopg text at 503).

So the base-class test proves the guarantee for a class no request ever
reaches, and the divergence it was written to prevent already exists one file
away. What actually saves the live path is that ``authentication_failed`` —
inherited, not overridden — re-raises anything ``denial_status`` rejects, so
the subclass's wider ``except`` only delays the fault by one method call.

That is a guarantee worth pinning rather than re-deriving: it is the only
reason the divergence is currently harmless, and a future edit to
``authentication_failed`` would break the shipping path without touching the
test that covers it. These tests therefore drive ``AuthenticateUser`` itself,
through a real ``DefaultExceptionHandler`` and a real ASGI ``send``, in
production mode.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from cara.exceptions import (
    DatabaseUnavailableException,
    InvalidTokenException,
    QueryException,
)
from cara.exceptions.handlers.DefaultExceptionHandler import DefaultExceptionHandler
from cara.middleware.http.AuthenticateUser import AuthenticateUser

# The exact string ``PostgresConnection`` wraps: host, address, port and the
# account whose authentication failed.
PSYCOPG_LEAK = (
    'connection to server at "db-prod.internal" (10.4.2.11), port 5432 failed: '
    'FATAL:  password authentication failed for user "cara_api"'
)


class _Guard:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def user(self):
        raise self._error


class _AuthManager:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def guard(self, _name: str) -> _Guard:
        return _Guard(self._error)


class _Application:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def make(self, binding: str) -> _AuthManager:
        assert binding == "auth"
        return _AuthManager(self._error)


class _ProdConfig:
    """``app.debug`` off — the configuration a production worker runs."""

    @staticmethod
    def __call__(key, default=None):
        if key == "app.debug":
            return False
        return default


class _SendRecorder:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    async def __call__(self, message: dict[str, Any]) -> None:
        self.messages.append(message)

    def status(self) -> int | None:
        for m in self.messages:
            if m.get("type") == "http.response.start":
                return int(m.get("status") or 0)
        return None

    def raw_body(self) -> bytes:
        for m in self.messages:
            if m.get("type") == "http.response.body":
                return bytes(m.get("body") or b"")
        return b""


def _middleware(error: Exception) -> AuthenticateUser:
    """Build without the provider boot the base ``Middleware.__init__``
    triggers."""
    middleware = AuthenticateUser.__new__(AuthenticateUser)
    middleware.application = _Application(error)
    middleware.guards = ["jwt"]
    return middleware


async def _next(_request):  # pragma: no cover - never reached in these tests
    raise AssertionError("the pipeline must not continue past a failed guard")


def _drive(error: Exception) -> tuple[str, int | None, bytes]:
    """Raise ``error`` from the guard and return what reached the socket.

    ``application=None`` on the handler routes through
    ``send_manual_response``, a real ``json.dumps`` — nothing here
    hand-assembles the response body.
    """
    from cara import configuration as _cfg

    middleware = _middleware(error)
    scope = {
        "type": "http",
        "scheme": "http",
        "method": "GET",
        "path": "/api/me",
        "headers": [],
        "client": None,
    }

    original = _cfg.config
    _cfg.config = _ProdConfig()  # type: ignore[assignment]
    try:
        handler = DefaultExceptionHandler(application=None)
        assert handler.is_debug_mode() is False, (
            "this test only proves anything in production mode"
        )

        async def _run() -> tuple[str, int | None, bytes]:
            try:
                response = await middleware.handle(MagicMock(), _next)
            except Exception as escaped:  # noqa: BLE001 - the ASGI server's job
                recorder = _SendRecorder()
                await handler.handle(escaped, None, scope, None, recorder)
                return ("envelope", recorder.status(), recorder.raw_body())
            return ("middleware", response.get_status_code(), response.content)

        return asyncio.run(_run())
    finally:
        _cfg.config = original  # type: ignore[assignment]


class TestAFaultReachesTheCallerRedacted:
    """The subclass's wider ``except`` must not become a shorter path to the
    caller than the base class's."""

    def test_a_dead_database_is_a_redacted_503_not_an_auth_failure(self) -> None:
        """``AuthenticateUser`` classifies this as a guard denial —
        ``is_http_exception`` is True — and hands it to
        ``authentication_failed`` as ``last_error``. Only the inherited
        ``denial_status`` gate stops it there.

        Pre-fix this answered ``401 {"error": "<psycopg text>", "type":
        "authentication_error"}``: the connection host, its address and the
        account whose password failed, to an unauthenticated caller, in
        production, with debug off — and the outage reported itself as an
        authentication error while every 5xx alert stayed silent.
        """
        answered_by, status, raw = _drive(
            DatabaseUnavailableException(PSYCOPG_LEAK, retry_after=1)
        )

        assert answered_by == "envelope"
        assert b"db-prod.internal" not in raw
        assert b"10.4.2.11" not in raw
        assert b"cara_api" not in raw
        assert b"password authentication failed" not in raw
        assert b"authentication_error" not in raw

        assert status == 503
        assert json.loads(raw.decode()) == {
            "error": "Internal server error",
            "type": "internal_error",
            "retry_after": 1,
        }

    def test_a_query_fault_does_not_carry_the_failing_sql_to_the_caller(
        self,
    ) -> None:
        """``QueryException`` declares no status, so the subclass re-raises
        it at the loop. Pinned anyway: the two exception families take
        different routes out of ``handle`` and both have to end redacted."""
        answered_by, status, raw = _drive(
            QueryException("SELECT token FROM api_tokens WHERE secret = 'abc'")
        )

        assert answered_by == "envelope"
        assert b"api_tokens" not in raw
        assert b"authentication_error" not in raw
        assert status == 500
        assert json.loads(raw.decode()) == {
            "error": "Internal server error",
            "type": "internal_error",
        }


class TestAGenuineDenialIsStillADenial:
    """Fail-closed must not become fail-loud: narrowing what counts as a
    denial has to leave real credential rejections answering 401."""

    def test_an_expired_token_is_a_401_with_its_own_message(self) -> None:
        answered_by, status, raw = _drive(InvalidTokenException("Token has expired"))

        assert answered_by == "middleware"
        assert status == 401
        assert json.loads(raw.decode()) == {
            "error": "Token has expired",
            "type": "authentication_error",
        }

    @pytest.mark.parametrize(
        "error, expected_status",
        [
            (InvalidTokenException("Token has expired"), 401),
            (DatabaseUnavailableException("outage", retry_after=1), 503),
            (QueryException("SELECT 1"), 500),
        ],
        ids=["denial", "http-facing-fault", "bare-fault"],
    )
    def test_the_status_says_whose_fault_it_was(
        self, error: Exception, expected_status: int
    ) -> None:
        """The classification, stated as the only thing a dashboard sees.
        Every one of these answered 401 before the fix, which is why a total
        database outage looked like a spike in bad credentials."""
        _answered_by, status, _raw = _drive(error)

        assert status == expected_status
