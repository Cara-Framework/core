"""On shutdown the consumer stops taking work BEFORE its in-flight job drains.

The subscription used to be cancelled only after the drain loop ended, so in
the turn where the in-flight job acknowledged, the broker could hand the slot
its next delivery and a worker that was told to stop started another job.
The subscription is now cancelled the moment shutdown is requested, while the
in-flight job keeps its open channel to settle on — and only once.
"""

from __future__ import annotations

import sys
import threading

import pytest

from cara.commands.core.QueueWorkCommand import QueueWorkCommand
from cara.queues.LeaseHeartbeat import LeaseHeartbeat
from tests.queues.test_queue_work_durable_consumer import _Manager

lifecycle = sys.modules["cara.commands.core._QueueWorkLifecycle"]


class _Exited(Exception):
    pass


def test_a_forced_exit_hands_back_every_held_lease_first(monkeypatch):
    """A job that ignores cancellation dies with the process; its leases are
    released before ``os._exit`` instead of lapsing a lease later."""
    events: list[str] = []
    LeaseHeartbeat(
        "queue_job_delivery:stuck",
        lease_seconds=120,
        renew=lambda: True,
        release=lambda: events.append("released"),
    ).start()

    def _exit(code):
        events.append(f"exit:{code}")
        raise _Exited

    monkeypatch.setattr(lifecycle.os, "_exit", _exit)

    with pytest.raises(_Exited):
        QueueWorkCommand._force_terminate_for_redelivery(
            reason="drain budget spent", signal_number=15
        )

    assert events == ["released", "exit:143"]
    assert LeaseHeartbeat.live() == []


def test_shutdown_cancels_the_subscription_while_the_job_still_drains():
    manager = _Manager(["sync"])
    command = QueueWorkCommand.__new__(QueueWorkCommand)
    command.shutdown_requested = False
    command.jobs_processed = 0
    command.jobs_failed = 0
    command._record_worker_outcome = lambda _outcome, _config: None
    observed: dict[str, list[str]] = {}

    class _DrainingProcessor:
        cancellation_registry = None

        def process_message(self, channel, method, body, **kwargs):
            command.shutdown_requested = True  # SIGTERM arrives mid-job
            deadline = threading.Event()
            for _ in range(200):
                if manager.consumer_channel.cancelled:
                    break
                deadline.wait(0.01)
            observed["cancelled_while_in_flight"] = list(
                manager.consumer_channel.cancelled
            )
            return "success"

    command._consume_queue_stream(
        queue_names=["sync"],
        connection_manager=manager,
        job_processor=_DrainingProcessor(),
        config={"timeout": 1},
    )

    assert observed["cancelled_while_in_flight"] == ["consumer-sync"]
    assert manager.consumer_channel.cancelled == ["consumer-sync"], (
        "the subscription must be cancelled exactly once"
    )
    assert manager.consumer_channel.is_open is False
