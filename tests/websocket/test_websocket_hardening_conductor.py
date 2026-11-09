"""The conductor is one object per process; a connection's state is not."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from cara.conductors.websocket.WebsocketConductor import WebsocketConductor
from cara.exceptions import WebSocketException
from cara.websocket.Socket import Socket


class _Application:
    """One controller per path; ``make`` answers what the conductor reads."""

    def __init__(self, controllers: dict) -> None:
        self.controllers = controllers
        self.router = SimpleNamespace(find=self._find)

    def make(self, key):
        if key == "middleware_ws":
            return SimpleNamespace(
                get_global_middleware=lambda: [],
                sort_by_priority=lambda middleware: middleware,
            )
        if key == "auth":
            return SimpleNamespace(guards={}, guard=lambda _name: SimpleNamespace())
        raise AssertionError(f"unexpected binding {key!r}")

    def _find(self, path: str, _kind: str):
        return SimpleNamespace(
            set_params_from_path=lambda _path: {},
            get_middleware=lambda: [],
            controller=SimpleNamespace(handle=self.controllers[path]),
        )


def _scope(path: str) -> dict:
    return {"type": "websocket", "path": path, "headers": [], "query_string": b""}


async def _never():
    await asyncio.Event().wait()


def _recorder(log: list[dict]):
    async def send(message: dict) -> None:
        log.append(message)

    return send


@pytest.mark.asyncio
async def test_one_connections_close_code_is_not_the_next_ones() -> None:
    """A typed failure used to park its code on the singleton, so every later
    uncaught controller error closed with the earlier connection's code."""

    async def forbidden(_arguments):
        raise WebSocketException("denied", 4003)

    async def crashed(_arguments):
        raise RuntimeError("boom")

    conductor = WebsocketConductor(_Application({"/a": forbidden, "/b": crashed}))
    first: list[dict] = []
    second: list[dict] = []

    await conductor.handle(_scope("/a"), _never, _recorder(first))
    with pytest.raises(RuntimeError):
        await conductor.handle(_scope("/b"), _never, _recorder(second))

    assert first[-1]["code"] == 4003
    assert second[-1]["code"] == 1011


@pytest.mark.asyncio
async def test_a_closing_connection_keeps_the_newer_ones_user() -> None:
    """Teardown clears the user and the JWT claims of the socket it holds."""
    release_older = asyncio.Event()
    release_newer = asyncio.Event()
    seen: dict = {}
    started = asyncio.Event()

    async def older(arguments):
        seen["older"] = arguments[0]
        await release_older.wait()

    async def newer(arguments):
        socket = arguments[0]
        socket.set_user("the-newer-user")
        socket.jwt_claims = {"sid": "s"}
        seen["newer"] = socket
        started.set()
        await release_newer.wait()
        seen["user_after_older_left"] = socket.user()
        seen["claims_after_older_left"] = socket.jwt_claims

    conductor = WebsocketConductor(_Application({"/old": older, "/new": newer}))
    old_task = asyncio.create_task(
        conductor.handle(_scope("/old"), _never, _recorder([]))
    )
    await asyncio.sleep(0)
    new_task = asyncio.create_task(
        conductor.handle(_scope("/new"), _never, _recorder([]))
    )
    await asyncio.wait_for(started.wait(), 1)

    release_older.set()
    await asyncio.wait_for(old_task, 1)
    release_newer.set()
    await asyncio.wait_for(new_task, 1)

    assert seen["user_after_older_left"] == "the-newer-user"
    assert seen["claims_after_older_left"] == {"sid": "s"}
    assert seen["older"].user() is None


@pytest.mark.asyncio
async def test_a_reply_to_a_client_that_stopped_reading_cannot_hold_the_close_out() -> (
    None
):
    """A controller's own reply had no bound and runs under the write lock,
    so a close from the registry or the conductor queued behind it for good."""
    sent: list[dict] = []

    async def send(message: dict) -> None:
        if message["type"] == "websocket.send":
            await asyncio.Event().wait()
        sent.append(message)

    socket = Socket(None, _scope("/x"), _never, send)
    socket._ws_connected = True
    socket.send_timeout = 0.01

    reply = asyncio.create_task(socket.send_json({"event": "ack"}))
    done, _pending = await asyncio.wait({reply}, timeout=0.5)
    reply.cancel()

    assert done, "the reply is still parked on the client"
    assert isinstance(reply.exception(), TimeoutError)
    assert socket.closed is False
    await asyncio.wait_for(socket.close(1013), 1)
    assert sent == [{"type": "websocket.close", "code": 1013}]


def test_the_conductor_keeps_no_connection_on_itself() -> None:
    assert not hasattr(WebsocketConductor(_Application({})), "socket")
