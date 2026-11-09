"""One connection's teardown must never touch another connection's socket.

``WebsocketConductorProvider`` binds ONE ``WebsocketConductor`` for the whole
process, and ``handle`` parks the connection on ``self.socket``. Live run
(2026-11-09, uvicorn + real websockets clients): a tab closing while a newer
connection was open closed THAT newer connection — another user's, another
workspace's — with 1000, before it had received a single frame.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from cara.conductors.websocket.WebsocketConductor import WebsocketConductor


class _Capsule:
    @staticmethod
    def get_global_middleware():
        return []

    @staticmethod
    def resolve_middleware(_alias):
        return None

    @staticmethod
    def sort_by_priority(middleware):
        return middleware


class _Application:
    """The container surface the conductor reads, with one route per path."""

    def __init__(self, routes: dict[str, asyncio.Event], started: dict) -> None:
        self.routes = routes
        self.started = started
        self.router = SimpleNamespace(find=self._find)

    def make(self, key):
        if key == "middleware_ws":
            return _Capsule()
        if key == "auth":
            return SimpleNamespace(guards={}, guard=lambda _name: SimpleNamespace())
        raise AssertionError(f"unexpected binding {key!r}")

    def _find(self, path: str, _kind: str):
        release = self.routes[path]
        started = self.started[path]

        async def controller(arguments):
            started.set()
            await release.wait()
            return arguments

        return SimpleNamespace(
            set_params_from_path=lambda _path: {},
            get_middleware=lambda: [],
            controller=SimpleNamespace(handle=controller),
        )


def _scope(path: str) -> dict:
    return {"type": "websocket", "path": path, "headers": [], "query_string": b""}


async def _never():
    await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_a_closing_connection_leaves_a_newer_connection_open() -> None:
    releases = {"/older": asyncio.Event(), "/newer": asyncio.Event()}
    started = {"/older": asyncio.Event(), "/newer": asyncio.Event()}
    conductor = WebsocketConductor(_Application(releases, started))
    sent: dict[str, list[dict]] = {"/older": [], "/newer": []}

    def sender(path: str):
        async def send(message: dict) -> None:
            sent[path].append(message)

        return send

    older = asyncio.create_task(
        conductor.handle(_scope("/older"), _never, sender("/older"))
    )
    await asyncio.wait_for(started["/older"].wait(), 1)
    newer = asyncio.create_task(
        conductor.handle(_scope("/newer"), _never, sender("/newer"))
    )
    await asyncio.wait_for(started["/newer"].wait(), 1)

    releases["/older"].set()
    await asyncio.wait_for(older, 1)

    try:
        assert [m for m in sent["/newer"] if m["type"] == "websocket.close"] == []
        assert [m["type"] for m in sent["/older"]][-1] == "websocket.close"
    finally:
        releases["/newer"].set()
        await asyncio.wait_for(newer, 1)
