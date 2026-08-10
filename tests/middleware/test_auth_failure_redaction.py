"""A crashed auth guard is a 5xx, not a 401 carrying the driver's message.

``ShouldAuthenticate.handle`` swallowed EVERY exception a guard raised
(``except Exception as e: last_error = e``) and then hand-rolled
``{"error": str(last_error), "type": "authentication_error"}`` at 401. That
response never passes through ``DefaultExceptionHandler.format_response``,
so the production redaction for unexpected 5xx never applied: a psycopg
error string carrying the connection DSN or the failing SQL fragment was
returned verbatim to an unauthenticated caller, in production, with debug
off. It also mis-classified the outage — a dead database presented to every
client and every dashboard as "401 authentication_error" while 5xx alerting
stayed silent.

``CanPerform`` already fixed the identical mistake on the authorization
half. §9: controllers never hand-roll error JSON — the envelope system
responds.

The typed branch was separately dead: it required BOTH ``.message`` and
``.status_code``, and ``CaraException`` carries neither by name, so every
typed failure was flattened to 401 — ``AccountLockedException`` lost its 429
and its ``retry_after``.

NARROWING TO ``is_http_exception`` DID NOT CLOSE IT. The framework's own
``DatabaseUnavailableException`` sets ``is_http_exception = True`` and
``status_code = 503``, and ``PostgresConnection`` raises it as
``DatabaseUnavailableException(str(e), retry_after=1)`` around the psycopg
``OperationalError`` — so the loop still classified a dead database as a
guard denial, and the renderer still accepted "any int status_code" and
wrote the body by hand::

    503 {"error": 'connection to server at "db-prod.internal" (10.4.2.11),
         port 5432 failed: FATAL:  password authentication failed for user
         "cara_api"', "type": "authentication_error"}

reproduced against the tree before this change. ``denial_status`` is now the
single predicate both halves consult: only a 4xx is a denial the guard can
assert; everything else is re-raised and the envelope system answers.
"""

from __future__ import annotations

import asyncio
import json
import sys
from typing import Any

import pytest

from cara.exceptions import (
    AccountLockedException,
    DatabaseUnavailableException,
    InvalidTokenException,
    QueryException,
)
from cara.exceptions.handlers.DefaultExceptionHandler import DefaultExceptionHandler
from cara.middleware.http.ShouldAuthenticate import ShouldAuthenticate, denial_status

# The package barrel shadows the submodule attribute, so the module object
# has to come from ``sys.modules`` rather than ``import ... as``.
module = sys.modules["cara.middleware.http.ShouldAuthenticate"]

# The exact string ``PostgresConnection`` wraps: host, port, and the account
# whose authentication failed. If any assertion below stops covering this,
# the leak is back.
PSYCOPG_LEAK = (
    'connection to server at "db-prod.internal" (10.4.2.11), port 5432 failed: '
    'FATAL:  password authentication failed for user "cara_api"'
)


class _CapturedResponse:
    def __init__(self) -> None:
        self.body: dict | None = None
        self.status_code: int | None = None

    def json(self, body: dict, status: int = 200) -> _CapturedResponse:
        self.body = body
        self.status_code = status
        return self


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


def _middleware(error: Exception) -> ShouldAuthenticate:
    """Build without the provider boot the base ``Middleware.__init__``
    triggers."""
    middleware = ShouldAuthenticate.__new__(ShouldAuthenticate)
    middleware.application = _Application(error)
    middleware.guards = ["jwt"]
    return middleware


async def _next(_request):  # pragma: no cover - never reached in these tests
    raise AssertionError("the pipeline must not continue past a failed guard")


def _failure_body(
    monkeypatch: pytest.MonkeyPatch, last_error: Exception | None
) -> _CapturedResponse:
    captured = _CapturedResponse()

    class _Factory:
        def __init__(self, _application) -> None:
            pass

        def json(self, body: dict, status: int = 200) -> _CapturedResponse:
            return captured.json(body, status)

    monkeypatch.setattr(module, "Response", _Factory)
    middleware = ShouldAuthenticate.__new__(ShouldAuthenticate)
    middleware.application = object()
    middleware.authentication_failed(request=None, last_error=last_error)
    return captured


class TestDenialStatusIsTheSinglePredicate:
    """One predicate, consulted by the loop and by the renderer. They
    drifted apart before and the gap between them was the leak."""

    def test_a_4xx_taxonomy_exception_is_a_denial(self) -> None:
        assert denial_status(InvalidTokenException("Token has expired")) == 401
        assert denial_status(AccountLockedException("locked")) == 429

    def test_a_5xx_http_facing_exception_is_not_a_denial(self) -> None:
        """The whole finding in one line: ``DatabaseUnavailableException``
        passes ``is_http_exception`` and fails this."""
        error = DatabaseUnavailableException(PSYCOPG_LEAK, retry_after=1)
        assert getattr(error, "is_http_exception", False) is True
        assert denial_status(error) is None

    def test_an_exception_without_a_status_is_not_a_denial(self) -> None:
        assert denial_status(QueryException("SELECT secret FROM users")) is None
        assert denial_status(RuntimeError("boom")) is None

    def test_a_bool_status_is_not_a_status(self) -> None:
        """``True`` is an ``int`` in Python. A truthy flag mistaken for a
        status must not be read as HTTP 1."""

        class _Flagged(Exception):
            status_code = True

        assert denial_status(_Flagged()) is None


class TestNonAuthGuardFailuresPropagate:
    def test_a_database_error_is_not_converted_into_a_401(self) -> None:
        """Pinned wrong behaviour: this answered 401 with the raw psycopg
        message — connection DSN included — to an unauthenticated caller."""
        leak = RuntimeError(
            "connection failed: host=db-prod user=cara password=hunter2 dbname=app"
        )
        middleware = _middleware(leak)

        with pytest.raises(RuntimeError) as excinfo:
            asyncio.run(middleware.handle(object(), _next))

        assert excinfo.value is leak

    def test_a_configuration_error_is_not_converted_into_a_401(self) -> None:
        """A CaraException that is not HTTP-facing is still a server fault."""
        from cara.exceptions import AuthenticationConfigurationException

        middleware = _middleware(AuthenticationConfigurationException("no jwt secret"))

        with pytest.raises(AuthenticationConfigurationException):
            asyncio.run(middleware.handle(object(), _next))

    def test_an_http_facing_5xx_is_not_converted_into_a_guard_denial(self) -> None:
        """Reproduced against the tree BEFORE this change: the loop
        accepted it (``is_http_exception`` is True) and the renderer wrote
        ``503 {"error": "<psycopg text>", "type": "authentication_error"}``
        by hand, skipping the redaction that exists for exactly that
        string."""
        outage = DatabaseUnavailableException(PSYCOPG_LEAK, retry_after=1)
        middleware = _middleware(outage)

        with pytest.raises(DatabaseUnavailableException) as excinfo:
            asyncio.run(middleware.handle(object(), _next))

        assert excinfo.value is outage

    def test_a_taxonomy_rejection_still_answers_401(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The narrowing must not turn a genuine credential rejection into
        a 500 — that is the whole point of the taxonomy."""
        captured = _CapturedResponse()

        class _Factory:
            def __init__(self, _application) -> None:
                pass

            def json(self, body: dict, status: int = 200) -> _CapturedResponse:
                return captured.json(body, status)

        monkeypatch.setattr(module, "Response", _Factory)
        middleware = _middleware(InvalidTokenException("Token has expired"))

        asyncio.run(middleware.handle(object(), _next))

        assert captured.status_code == 401
        assert captured.body == {
            "error": "Token has expired",
            "type": "authentication_error",
        }


class TestTypedExceptionsKeepTheirOwnStatus:
    def test_account_locked_keeps_429_and_retry_after(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pinned wrong behaviour: the ``.message`` requirement made this
        branch dead for the framework's own taxonomy, so a locked account
        answered 401 with no ``retry_after``."""
        captured = _failure_body(
            monkeypatch, AccountLockedException("Account locked", retry_after_seconds=90)
        )

        assert captured.status_code == 429
        assert captured.body == {
            "error": "Account locked",
            "type": "rate_limit_exceeded",
            "retry_after": 90,
        }


class TestUntrustedExceptionTextIsWithheld:
    def test_an_arbitrary_exception_is_not_rendered_here_at_all(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """RE-PINNED (§11). This used to assert a 401 body of
        ``"Authentication required"`` — safe text, but still the wrong
        answer: it told an unauthenticated caller their credentials had
        failed when the truth was that the guard crashed, and it kept the
        outage out of every 5xx alert. ``authentication_failed`` renders
        denials only; a fault goes to the envelope system."""

        class _DriverError(Exception):
            pass

        error = _DriverError("host=db-prod password=x")

        with pytest.raises(_DriverError) as excinfo:
            _failure_body(monkeypatch, error)

        assert excinfo.value is error

    def test_a_bare_cara_exception_message_is_not_echoed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``QueryException`` carries the failing SQL and declares no
        status — the old ``isinstance(last_error, RuntimeError |
        CaraException)`` branch echoed it verbatim at 401."""
        error = QueryException("SELECT token FROM api_tokens WHERE secret = 'abc'")

        with pytest.raises(QueryException):
            _failure_body(monkeypatch, error)

    def test_a_runtime_error_subclass_is_not_a_deliberate_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``NotImplementedError`` and ``RecursionError`` ARE
        ``RuntimeError`` subclasses. Only the exact type is a caller's
        chosen text; a subclass is always a fault."""
        with pytest.raises(NotImplementedError):
            _failure_body(monkeypatch, NotImplementedError("driver stub"))

    def test_a_deliberate_runtime_error_message_still_round_trips(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``authentication_failed`` is a public surface: product
        middlewares pass a ``RuntimeError`` whose message is a chosen,
        user-facing string, and both dashboards branch on that 401."""
        captured = _failure_body(monkeypatch, RuntimeError("Session revoked or expired"))

        assert captured.status_code == 401
        assert captured.body == {
            "error": "Session revoked or expired",
            "type": "authentication_error",
        }

    def test_no_guard_error_still_answers_the_canonical_401(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _failure_body(monkeypatch, None)

        assert captured.status_code == 401
        assert captured.body == {
            "error": "Authentication required",
            "type": "authentication_error",
        }


# ── The wire, end to end ──────────────────────────────────────────────
#
# Everything above asserts on the middleware's decision. What actually
# leaked was BYTES. Drive the real hop: guard raises inside ``handle``,
# the exception unwinds the middleware, ``DefaultExceptionHandler`` builds
# and json-serialises the body, and an ASGI ``send`` records the frames.
# ``application=None`` routes through ``send_manual_response`` — a real
# ``json.dumps`` — so nothing here hand-assembles the response dict.


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

    def headers(self) -> dict[bytes, bytes]:
        for m in self.messages:
            if m.get("type") == "http.response.start":
                return {bytes(k): bytes(v) for k, v in (m.get("headers") or [])}
        return {}

    def raw_body(self) -> bytes:
        for m in self.messages:
            if m.get("type") == "http.response.body":
                return bytes(m.get("body") or b"")
        return b""


class _ProdConfig:
    """``app.debug`` off — the configuration a production worker runs."""

    @staticmethod
    def __call__(key, default=None):
        if key == "app.debug":
            return False
        return default


def _drive_to_the_wire(error: Exception) -> _SendRecorder:
    """Raise ``error`` from a guard and record what an unauthenticated
    caller receives on the socket."""
    from cara import configuration as _cfg

    middleware = _middleware(error)
    recorder = _SendRecorder()
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

        async def _run() -> None:
            try:
                await middleware.handle(object(), _next)
            except Exception as escaped:  # noqa: BLE001 - the ASGI server's job
                await handler.handle(escaped, None, scope, None, recorder)
            else:  # pragma: no cover - would mean the leak is back
                raise AssertionError(
                    "the guard fault never reached the exception handler"
                )

        asyncio.run(_run())
    finally:
        _cfg.config = original  # type: ignore[assignment]

    return recorder


def test_a_dead_database_reaches_the_caller_as_a_redacted_503() -> None:
    """The reproduction, end to end.

    Before: 503 with the psycopg text as ``error`` and
    ``type: authentication_error``, written by ``authentication_failed``.
    After: the fault unwinds, ``format_response`` applies the 5xx-prod
    redaction, and the documented ``retry_after`` survives into both the
    body and the ``Retry-After`` header.
    """
    recorder = _drive_to_the_wire(
        DatabaseUnavailableException(PSYCOPG_LEAK, retry_after=1)
    )

    raw = recorder.raw_body()
    assert b"db-prod.internal" not in raw
    assert b"10.4.2.11" not in raw
    assert b"cara_api" not in raw
    assert b"password authentication failed" not in raw

    assert recorder.status() == 503
    assert json.loads(raw.decode()) == {
        "error": "Internal server error",
        "type": "internal_error",
        "retry_after": 1,
    }
    assert recorder.headers().get(b"retry-after") == b"1"


def test_a_crashed_guard_never_reports_itself_as_an_auth_failure() -> None:
    """The observability half. Classifying an outage as
    ``authentication_error`` kept every 5xx alert silent while the database
    was down."""
    recorder = _drive_to_the_wire(
        QueryException("SELECT token FROM api_tokens WHERE secret = 'abc'")
    )

    raw = recorder.raw_body()
    assert b"api_tokens" not in raw
    assert b"authentication_error" not in raw
    assert recorder.status() == 500
    assert json.loads(raw.decode()) == {
        "error": "Internal server error",
        "type": "internal_error",
    }
