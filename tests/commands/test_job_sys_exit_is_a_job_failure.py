"""A job that calls ``sys.exit`` fails like any other job.

``SystemExit`` is a BaseException: no ``except Exception`` between the job and
the consumer thread caught it. The delivery was never settled (no attempt
spent, redelivered once its lease lapsed — for ever, never reaching the DLQ)
and the exception ended the slot's thread. It is now a ``QueueException`` job
failure that goes through the failure router.
"""

from __future__ import annotations

import builtins
import importlib
import sys
from types import SimpleNamespace

import pendulum
import pytest

from cara.commands.core.ActiveJobCancellationRegistry import ActiveJobCancellationRegistry
from cara.commands.core.JobProcessor import JobProcessor
from cara.exceptions import QueueException
from cara.queues.delivery import DeliveryClaim


class _ExitingJob:
    queue = "sync"

    async def handle(self) -> None:
        sys.exit(3)


def test_the_execution_boundary_turns_sys_exit_into_a_job_failure() -> None:
    async def _exits():
        sys.exit(3)

    with pytest.raises(QueueException, match=r"SystemExit\(3\)") as raised:
        JobProcessor._execute_async_job_with_timeout(_exits, (), 5)

    assert isinstance(raised.value.__cause__, SystemExit)


def test_a_job_that_exits_is_settled_as_a_failure(monkeypatch) -> None:
    execution = importlib.import_module("cara.commands.core._JobExecution")
    payload = {
        "job_id": "66666666-6666-4666-8666-666666666666",
        "db_job_id": 92,
        "queue": "sync",
        "timeout_seconds": 30,
        "attempts": 0,
        "_tenant": 5,
        "_tenant_mode": "tenant",
    }
    events: list[str] = []
    store = SimpleNamespace(
        execution_heartbeat_seconds=30,
        claim_execution=lambda *, body, payload: DeliveryClaim(
            "claimed", "lease", lease_deadline=pendulum.now("UTC").add(seconds=600)
        ),
        renew_execution=lambda *_a, **_k: True,
        execution_timeout_for=lambda _cls: 30,
        abandon_execution=lambda _job_id, _token: events.append("abandon"),
    )
    tracker = SimpleNamespace(
        is_job_completed=lambda _id: False,
        update_job_status_strict=lambda _id, status: events.append(f"tracker:{status}"),
    )
    quiet = SimpleNamespace(
        **{
            level: lambda *_a, **_k: None
            for level in ("debug", "info", "warning", "error")
        }
    )
    application = SimpleNamespace(
        has=lambda key: key == "JobTracker",
        call=lambda method, *args: method(*args),
        make=lambda key: {
            "queue": SimpleNamespace(
                driver=lambda *_a, **_k: SimpleNamespace(delivery_store=store)
            ),
            "JobTracker": tracker,
            "logger": quiet,
        }[key],
    )
    monkeypatch.setattr(builtins, "app", lambda: application, raising=False)
    monkeypatch.setattr(
        execution.SignedJsonJobSerializer,
        "inspect_envelope",
        classmethod(lambda cls, *_a, **_k: {"payload": payload}),
    )
    monkeypatch.setattr(
        execution.SignedJsonJobSerializer,
        "deserialize_verified",
        classmethod(
            lambda cls, verified, **_k: {**verified, "obj": _ExitingJob(), "args": ()}
        ),
    )
    monkeypatch.setattr(execution.observability, "capture_exception", lambda _exc: None)
    routed: dict = {}

    def _router(**kwargs) -> str:
        routed["exc"] = kwargs["exc"]
        return "retry_scheduled"

    monkeypatch.setattr(JobProcessor, "_route_failed_message", staticmethod(_router))

    outcome = JobProcessor.process_message(
        SimpleNamespace(basic_ack=lambda **_k: None, basic_nack=lambda **_k: None),
        SimpleNamespace(delivery_tag=1),
        b"x",
        queue_name="sync",
        cancellation_registry=ActiveJobCancellationRegistry(),
    )

    assert outcome == "failure"
    # Through the failure router: an attempt is spent and the DLQ is reachable.
    assert isinstance(routed["exc"], QueueException)
    assert isinstance(routed["exc"].__cause__, SystemExit)
    assert "abandon" not in events
