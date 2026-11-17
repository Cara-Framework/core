"""The realtime door and the event door: after commit, never on rollback,
transport/listener failures swallowed in one place."""

from __future__ import annotations

import asyncio

import pytest

import cara.broadcasting.AfterCommit as broadcast_door
import cara.events.AfterCommit as event_door
from cara.broadcasting import BroadcastEvent, Channel, broadcast_after_commit
from cara.eloquent import DatabaseManager
from cara.eloquent.connections.ConnectionResolver import (
    _get_after_commit_registry,
    _get_registry,
)
from cara.events import fire_after_commit


class _Conn:
    def __init__(self) -> None:
        self.transaction_level = 0

    def begin(self):
        self.transaction_level += 1
        return self

    def commit(self):
        self.transaction_level = max(0, self.transaction_level - 1)
        return self

    def rollback(self):
        self.transaction_level = max(0, self.transaction_level - 1)
        return self

    def close_connection(self):
        pass


class _Frame(BroadcastEvent):
    def __init__(self) -> None:
        super().__init__(channels=[Channel("public.test")], event_name="test.frame")


@pytest.fixture
def db(monkeypatch):
    _get_registry().clear()
    _get_after_commit_registry().clear()
    dm = DatabaseManager("app", {"app": {"driver": "sqlite"}})
    dm._ensure_resolver()._create_connection_instance = lambda name: _Conn()
    monkeypatch.setattr(broadcast_door, "DB", dm)
    monkeypatch.setattr(event_door, "DB", dm)
    yield dm
    _get_registry().clear()
    _get_after_commit_registry().clear()


@pytest.fixture
def sent(monkeypatch):
    frames: list = []

    async def _send(event):
        frames.append(event)

    monkeypatch.setattr(broadcast_door, "broadcast_event_async", _send)
    return frames


def test_frame_waits_for_the_outermost_commit(db, sent) -> None:
    with db.transaction():
        with db.transaction():
            broadcast_after_commit(_Frame())
        assert sent == []
    assert len(sent) == 1


def test_rolled_back_write_announces_nothing(db, sent) -> None:
    with pytest.raises(RuntimeError), db.transaction():
        broadcast_after_commit(_Frame())
        raise RuntimeError("rollback")
    assert sent == []


def test_no_transaction_sends_now(db, sent) -> None:
    broadcast_after_commit(_Frame())
    assert len(sent) == 1


def test_transport_error_is_swallowed_and_logged(db, monkeypatch) -> None:
    async def _boom(event):
        raise ConnectionError("redis down")

    warnings: list = []
    monkeypatch.setattr(broadcast_door, "broadcast_event_async", _boom)
    monkeypatch.setattr(broadcast_door.Log, "warning", lambda *a, **k: warnings.append(a))
    with db.transaction():
        broadcast_after_commit(_Frame())  # must not raise at commit
    assert warnings and "not sent" in warnings[0][0]


def test_inside_a_running_loop_the_send_is_scheduled(db, sent) -> None:
    async def _run() -> None:
        with db.transaction():
            broadcast_after_commit(_Frame())
        await asyncio.sleep(0)

    asyncio.run(_run())
    assert len(sent) == 1


def test_non_broadcast_is_refused(db) -> None:
    with pytest.raises(TypeError):
        broadcast_after_commit(object())  # type: ignore[arg-type]


def test_event_fires_after_commit_and_not_on_rollback(db, monkeypatch) -> None:
    fired: list = []
    monkeypatch.setattr(event_door.Event, "fire_sync", staticmethod(fired.append))
    with pytest.raises(RuntimeError), db.transaction():
        fire_after_commit("rolled-back")
        raise RuntimeError("rollback")
    with db.transaction():
        fire_after_commit("committed")
        assert fired == []
    assert fired == ["committed"]


def test_listener_failure_after_commit_is_logged_not_raised(db, monkeypatch) -> None:
    def _boom(event):
        raise ValueError("listener bug")

    errors: list = []
    monkeypatch.setattr(event_door.Event, "fire_sync", staticmethod(_boom))
    monkeypatch.setattr(event_door.Log, "error", lambda *a, **k: errors.append(a))
    with db.transaction():
        fire_after_commit("x")
    assert errors
