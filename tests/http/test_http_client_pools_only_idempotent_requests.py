"""Idempotent outbound requests reuse a kept-alive connection; nothing else does.

Live defect: ``PendingRequest`` built an ``httpx.AsyncClient`` per call, so
every request opened a new TCP (and TLS) connection: 50 sequential GETs to
one host opened 50 connections. Pooling had been skipped because a stale
kept-alive connection changes how a request that must not be re-sent fails.

Now GET/HEAD/OPTIONS/PUT/DELETE share one client per running loop; a
failure caused by a reused connection the server closed is retried once on a
fresh connection; POST/PATCH and requests with options the shared client
cannot carry per request keep a fresh connection per call.

The server below is a raw asyncio HTTP/1.1 server on 127.0.0.1 in the test's
own loop: it counts accepted connections and, when asked, drops a kept-alive
connection on its second request without answering (the stale race).
"""

from __future__ import annotations

import asyncio
import importlib

import httpx
import pytest

from cara.http.client import PendingRequest


class _Server:
    def __init__(self, *, drop_reused: bool = False, drop_first: bool = False):
        self.connections = 0
        self.requests: list[bytes] = []
        self.drop_reused = drop_reused
        self.drop_first = drop_first
        self.url = ""
        self._server: asyncio.Server | None = None

    async def __aenter__(self) -> _Server:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        port = self._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/x"
        return self

    async def __aexit__(self, *_exc) -> None:
        assert self._server is not None
        self._server.close()

    async def _serve(self, reader, writer) -> None:
        self.connections += 1
        served = 0
        try:
            while True:
                head = await reader.readuntil(b"\r\n\r\n")
                length = 0
                for line in head.split(b"\r\n"):
                    if line.lower().startswith(b"content-length:"):
                        length = int(line.split(b":", 1)[1])
                if length:
                    await reader.readexactly(length)
                self.requests.append(head)
                if (self.drop_reused and served >= 1) or self.drop_first:
                    return
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n"
                    b"Set-Cookie: session=leak\r\n\r\nok"
                )
                await writer.drain()
                served += 1
        except asyncio.IncompleteReadError:
            return
        finally:
            writer.close()


def _run(scenario):
    return asyncio.run(scenario())


def test_sequential_gets_share_one_connection() -> None:
    async def scenario():
        async with _Server() as server:
            for _ in range(20):
                response = await PendingRequest().get(server.url)
                assert response.status_code == 200
            return server.connections

    assert _run(scenario) == 1


@pytest.mark.parametrize("method", ["post", "patch"])
def test_post_and_patch_open_a_fresh_connection_per_call(method) -> None:
    async def scenario():
        async with _Server() as server:
            for _ in range(5):
                await getattr(PendingRequest(), method)(server.url, content=b"x")
            return server.connections

    assert _run(scenario) == 5


def test_a_kept_alive_connection_the_server_dropped_is_retried_once_fresh() -> None:
    async def scenario():
        async with _Server(drop_reused=True) as server:
            first = await PendingRequest().get(server.url)
            second = await PendingRequest().get(server.url)
            return first.status_code, second.status_code, server.connections

    # The second GET went out on the reused connection, lost it, and was
    # answered on a fresh one: no error reaches the caller.
    assert _run(scenario) == (200, 200, 2)


def test_a_fresh_connection_failure_surfaces_exactly_as_before() -> None:
    async def scenario():
        async with _Server(drop_first=True) as server:
            with pytest.raises(httpx.RemoteProtocolError):
                await PendingRequest().get(server.url)
            return server.connections

    # Not a stale connection: no second attempt.
    assert _run(scenario) == 1


def test_a_post_on_a_dropped_connection_is_not_resent() -> None:
    async def scenario():
        async with _Server(drop_first=True) as server:
            with pytest.raises(httpx.RemoteProtocolError):
                await PendingRequest().post(server.url, content=b"x")
            return len(server.requests)

    assert _run(scenario) == 1


def test_a_cookie_set_for_one_caller_never_rides_on_another() -> None:
    async def scenario():
        async with _Server() as server:
            await PendingRequest().get(server.url)
            await PendingRequest().get(server.url)
            return server.requests

    requests = _run(scenario)
    assert all(b"session=leak" not in head for head in requests)


def test_builder_headers_go_with_their_request_only() -> None:
    async def scenario():
        async with _Server() as server:
            await PendingRequest().with_token("secret").get(server.url)
            await PendingRequest().get(server.url)
            return server.requests, server.connections

    requests, connections = _run(scenario)
    assert b"authorization: bearer secret" in requests[0].lower()
    assert b"authorization" not in requests[1].lower()
    assert connections == 1


@pytest.mark.parametrize(
    "options",
    [{"follow_redirects": True}, {"extensions": {}}],
)
def test_options_the_shared_client_cannot_carry_keep_a_fresh_connection(
    options,
) -> None:
    async def scenario():
        async with _Server() as server:
            for _ in range(3):
                await PendingRequest().get(server.url, **options)
            return server.connections

    assert _run(scenario) == 3


def test_each_loop_has_its_own_client_closed_when_the_loop_shuts_down() -> None:
    shared = importlib.import_module("cara.http.client._SharedClients")

    async def scenario():
        async with _Server() as server:
            await PendingRequest().get(server.url)
            loop = asyncio.get_running_loop()
            return shared._CLIENTS[loop][0]

    first = _run(scenario)
    second = _run(scenario)

    assert first is not second
    assert first.is_closed
    assert second.is_closed
