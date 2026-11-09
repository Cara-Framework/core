"""A refused WebSocket ends with its close code, not in the ASGI server's error log.

Anything ``WebsocketConductor.handle`` lets escape reaches uvicorn, which writes
``Exception in ASGI application`` and the whole traceback at error level. An
anonymous caller could fill the error log just by connecting without a token.
"""

from __future__ import annotations

import asyncio
import importlib
from types import SimpleNamespace

import pytest

from cara.conductors.websocket.WebsocketConductor import WebsocketConductor
from cara.exceptions import WebSocketException
from cara.routing import Route, Router

conductor_module = importlib.import_module("cara.conductors.websocket.WebsocketConductor")


class _Refusing:
    """Global middleware that refuses the connection the way Authenticate does."""

    def __init__(self, code: int) -> None:
        self.code = code
        self.terminated_with = None

    async def handle(self, _socket, _next):
        raise WebSocketException("refused", self.code)

    async def terminate(self, socket) -> None:
        self.terminated_with = socket


class _Crashing:
    async def handle(self, _socket, _next):
        raise RuntimeError("boom")


class _Application:
    def __init__(self, middleware: list) -> None:
        self.middleware = middleware
        self.router = SimpleNamespace(find=self._unreachable)

    def make(self, key):
        if key == "middleware_ws":
            return SimpleNamespace(get_global_middleware=lambda: self.middleware)
        if key == "auth":
            return SimpleNamespace(guards={}, guard=lambda _name: SimpleNamespace())
        raise AssertionError(f"unexpected binding {key!r}")

    def _unreachable(self, _path: str, _kind: str):
        raise AssertionError("a refused connection never reaches routing")


class _RecordingLog:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def debug(self, *_args, **_kwargs) -> None:
        return None

    def error(self, message, *_args, **_kwargs) -> None:
        self.errors.append(message)


@pytest.fixture
def log(monkeypatch) -> _RecordingLog:
    recording = _RecordingLog()
    monkeypatch.setattr(conductor_module, "Log", recording)
    return recording


def _scope() -> dict:
    return {
        "type": "websocket",
        "path": "/ws/tenant",
        "headers": [],
        "query_string": b"",
    }


async def _never():
    await asyncio.Event().wait()


def _recorder(sent: list[dict]):
    async def send(message: dict) -> None:
        sent.append(message)

    return send


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [4002, 4003, 4006, 4008])
async def test_a_refused_connection_closes_with_its_code_and_does_not_propagate(
    log: _RecordingLog, code: int
) -> None:
    sent: list[dict] = []
    conductor = WebsocketConductor(_Application([_Refusing(code)]))

    await conductor.handle(_scope(), _never, _recorder(sent))

    assert [message["type"] for message in sent] == [
        "websocket.accept",
        "websocket.close",
    ]
    assert sent[-1]["code"] == code
    assert log.errors == [], "a refusal is not a server error"


@pytest.mark.asyncio
async def test_a_refused_connection_still_runs_terminable_middleware(
    log: _RecordingLog,
) -> None:
    refusing = _Refusing(4006)
    conductor = WebsocketConductor(_Application([refusing]))

    await conductor.handle(_scope(), _never, _recorder([]))

    assert refusing.terminated_with is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/api/health", "/not/registered"])
async def test_a_path_with_no_websocket_route_closes_1008_and_does_not_propagate(
    log: _RecordingLog, path: str
) -> None:
    """A path that only has HTTP routes makes the router answer "method not
    allowed", not "not found". For a WebSocket both mean there is no such route."""
    application = _Application([])
    application.router = Router(None, Route.get("/api/health", lambda: None))
    sent: list[dict] = []
    conductor = WebsocketConductor(application)
    scope = {**_scope(), "path": path}

    await conductor.handle(scope, _never, _recorder(sent))

    assert sent[-1]["code"] == 1008
    assert log.errors == [], "a misdirected client is not a server error"


@pytest.mark.asyncio
async def test_an_uncaught_error_is_still_logged_raised_and_closed_1011(
    log: _RecordingLog,
) -> None:
    sent: list[dict] = []
    conductor = WebsocketConductor(_Application([_Crashing()]))

    with pytest.raises(RuntimeError, match="boom"):
        await conductor.handle(_scope(), _never, _recorder(sent))

    assert sent[-1] == {
        "type": "websocket.close",
        "code": 1011,
        "reason": "Internal error",
    }
    assert log.errors == ["Error in WebSocket connection: %s"]
