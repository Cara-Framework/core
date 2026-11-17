"""``AuthorizationFence`` — an open socket stays inside the authority that admitted it."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from cara.websocket import AuthorizationFence, FenceCheck


class _Socket:
    def __init__(self) -> None:
        self.is_connected = True
        self.closed: list[tuple[int, str]] = []

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed.append((code, reason))
        self.is_connected = False


def _answers(*values):
    queue = list(values)

    async def ask():
        value = queue.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value

    return ask


def test_closes_with_the_first_failing_checks_code_and_skips_the_rest() -> None:
    socket = _Socket()
    later = AsyncMock(return_value=True)
    fence = AuthorizationFence(
        socket,
        [
            FenceCheck("session", _answers(True, False), 4006, "Session revoked"),
            FenceCheck("membership", later, 4007, "Access revoked"),
        ],
        interval_seconds=0,
    )

    asyncio.run(fence.hold())

    assert socket.closed == [(4006, "Session revoked")]
    # Round one asked both; round two stopped at the session.
    assert later.await_count == 1


def test_a_later_check_closes_with_its_own_code() -> None:
    socket = _Socket()
    fence = AuthorizationFence(
        socket,
        [
            FenceCheck("session", AsyncMock(return_value=True), 4006, "Session revoked"),
            FenceCheck("membership", _answers(False), 4007, "Access revoked"),
        ],
        interval_seconds=0,
    )

    asyncio.run(fence.hold())

    assert socket.closed == [(4007, "Access revoked")]


def test_an_error_while_asking_fails_closed() -> None:
    socket = _Socket()
    fence = AuthorizationFence(
        socket,
        [
            FenceCheck(
                "session", _answers(RuntimeError("db down")), 4006, "Session revoked"
            )
        ],
        interval_seconds=0,
    )

    asyncio.run(fence.hold())

    assert socket.closed == [(4006, "Session revoked")]


def test_a_socket_that_closed_on_its_own_is_never_asked_or_closed_again() -> None:
    socket = _Socket()
    ask = AsyncMock(return_value=True)

    async def scenario() -> None:
        task = AuthorizationFence(
            socket, [FenceCheck("session", ask, 4006, "x")], interval_seconds=0.01
        ).start()
        socket.is_connected = False
        await asyncio.wait_for(task, 1)

    asyncio.run(scenario())

    assert socket.closed == []
    ask.assert_not_awaited()


def test_cancelling_the_fence_does_not_close_the_socket() -> None:
    socket = _Socket()

    async def scenario() -> None:
        task = AuthorizationFence(
            socket,
            [FenceCheck("session", AsyncMock(return_value=True), 4006, "x")],
            interval_seconds=3600,
        ).start()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert socket.closed == []


@pytest.mark.parametrize(
    ("checks", "interval"),
    [([], 30.0), ([FenceCheck("s", AsyncMock(), 4006, "x")], -1.0)],
)
def test_rejects_a_fence_that_could_never_fence(checks, interval) -> None:
    with pytest.raises(ValueError):
        AuthorizationFence(_Socket(), checks, interval_seconds=interval)
