"""One keep-alive ``httpx.AsyncClient`` per running event loop.

``PendingRequest`` used to build a client per call, so every outbound
request paid a TCP connect (and a TLS handshake on https) and the server's
keep-alive was thrown away. Only idempotent requests use this pool (see
``PendingRequest._poolable``): a stale kept-alive connection can fail after
the bytes left, and only a request that may be sent twice can be retried.

An async client is bound to the loop its connections were opened on, so the
pool is keyed by the running loop, never shared across loops (and so never
across threads, each of which runs its own loop). The client is closed when
its loop shuts down: a keeper async generator is registered with the loop,
and ``loop.shutdown_asyncgens()`` (which ``asyncio.run``, ``asyncio.Runner``
and uvicorn all call before closing a loop) finalizes it.

The shared client carries no per-request state: no default headers or
timeout (each request passes its own), a cookie jar that stores nothing (a
``Set-Cookie`` answered to one caller must not ride on another caller's
request), and no connection cap (a per-call client never waited for a pool
slot, so a cap would add ``PoolTimeout`` as a new failure).
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from collections.abc import AsyncIterator
from http.cookiejar import CookieJar, DefaultCookiePolicy
from typing import Any

import httpx


class _StoreNothingPolicy(DefaultCookiePolicy):
    """Refuses every cookie a response sets; sending is unaffected (the jar is
    empty, and a request's own ``cookies=`` are merged per request)."""

    def set_ok(self, cookie: Any, request: Any) -> bool:
        return False


_LOCK = threading.Lock()
# loop -> (client, keeper). Weak on the loop so a dropped loop drops its entry.
_CLIENTS: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop, tuple[httpx.AsyncClient, AsyncIterator[None]]
] = weakref.WeakKeyDictionary()


async def _keeper(client: httpx.AsyncClient) -> AsyncIterator[None]:
    """Suspended for the loop's lifetime; its ``finally`` closes the client
    when the loop finalizes its async generators at shutdown."""
    try:
        yield
    finally:
        await client.aclose()


async def _shared_client(verify: Any) -> httpx.AsyncClient:
    """The running loop's pooled client, built on first use."""
    loop = asyncio.get_running_loop()
    with _LOCK:
        entry = _CLIENTS.get(loop)
        if entry is not None and not entry[0].is_closed:
            return entry[0]
        client = httpx.AsyncClient(
            http2=True,
            verify=verify,
            cookies=CookieJar(policy=_StoreNothingPolicy()),
            limits=httpx.Limits(max_connections=None, max_keepalive_connections=20),
        )
        keeper = _keeper(client)
        _CLIENTS[loop] = (client, keeper)
    # First iteration registers the generator with this loop (asyncgen hooks);
    # the strong reference above keeps it alive until the loop finalizes it.
    await keeper.__anext__()
    return client
