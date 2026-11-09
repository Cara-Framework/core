"""The rest of the loops that end with a broadcast still pending.

``test_serve_and_realtime_job_loop_drain`` drives the worker, the sync
middleware runner and the terminal hooks. These are the other owners of a loop
that ends as soon as its coroutine returns (scheduler ticks, CLI commands,
``Event.fire_sync``), and what the drain must leave alone in the job's timeout.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import cara.commands.core._ScheduleRegistration as schedule_registration
from cara.broadcasting import helpers
from cara.commands.CommandRunner import CommandRunner
from cara.commands.core.JobProcessor import JobProcessor
from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand
from cara.events.Event import Event


class _Backend:
    """Stands in for ``facades.Broadcast``; a publish takes a real loop turn."""

    def __init__(self, delay: float = 0.0) -> None:
        self.frames: list[str] = []
        self.delay = delay

    async def broadcast(self, _channels: Any, event: str, _data: Any, **_kw: Any) -> None:
        await asyncio.sleep(self.delay)
        self.frames.append(event)


@pytest.fixture
def backend():
    fake = _Backend()
    with patch("cara.facades.Broadcast", fake):
        yield fake


class _ScheduledJob:
    central_job = True
    _tenant_mode = "central"
    _tenant_id = None

    async def handle(self) -> None:
        helpers.broadcast("private-orders", "schedule.tick", {})


def test_a_decorated_scheduled_job_publishes_before_its_loop_ends(backend):
    command = SimpleNamespace(application=SimpleNamespace(make=lambda target: target()))
    ScheduleWorkCommand._run_decorated_central_job(command, _ScheduledJob)
    assert backend.frames == ["schedule.tick"]


def test_a_dict_scheduled_job_publishes_before_its_loop_ends(backend):
    command = SimpleNamespace(
        application=SimpleNamespace(make=lambda target: target()),
        warning=lambda *_a: None,
    )
    spec = {
        "job": f"{__name__}._ScheduledJob",
        "trigger": "interval",
        "seconds": 60,
        "id": "probe",
    }
    with (
        patch.object(schedule_registration, "Schedule", MagicMock()) as schedule,
        patch.object(schedule_registration, "config", lambda _key, default=None: default),
    ):
        schedule_registration._register_scheduled_dict_job(command, spec)
        schedule.call.call_args.args[0]()
    assert backend.frames == ["schedule.tick"]


def test_an_async_command_publishes_before_its_loop_ends(backend):
    class _Command:
        def __init__(self, _application: Any) -> None:
            pass

        async def handle(self) -> None:
            helpers.broadcast("private-orders", "command.done", {})

    application = SimpleNamespace(call=lambda handle, **_kw: handle())
    runner = CommandRunner(application, instrument_commands=False)
    runner._make_callback(_Command, "probe", [])()
    assert backend.frames == ["command.done"]


def test_a_listener_broadcast_survives_fire_sync(backend):
    # No running loop, so ``fire_sync`` owns one for the dispatch alone.
    class _Happened:
        name = "orders.happened"

    events = Event()
    events.listen(
        _Happened.name,
        lambda _event: helpers.broadcast("private-orders", "order.happened", {}),
    )
    application = SimpleNamespace(make=lambda _name: events)
    with patch.object(Event, "_resolve_application", return_value=application):
        Event.fire_sync(_Happened())
    assert backend.frames == ["order.happened"]


def test_a_slow_redis_cannot_turn_a_finished_job_into_a_timed_out_one():
    # The job is done in no time and its timeout is far inside the drain's
    # bound: the drain sits outside the timeout, so the job still succeeded.
    async def _job() -> str:
        helpers.broadcast("private-orders", "order.updated", {})
        return "done"

    with patch("cara.facades.Broadcast", _Backend(delay=60)), patch("cara.facades.Log"):
        assert JobProcessor._execute_async_job_with_timeout(_job, (), 0.2) == "done"
