"""A replay that derives a key must fail as a replay, one row at a time.

``_replay_unique_key`` exists for a source row older than its job class's
current contract, and it builds that class from the row's kwargs. Those kwargs
are what the older contract signed, so the constructor can refuse them. The
operator doors guard a batch with ``except QueueException`` per row.
"""

from __future__ import annotations

from types import SimpleNamespace

import pendulum
import pytest

from cara.commands.core.QueueRetryCommand import QueueRetryCommand
from cara.exceptions import QueueException
from cara.queues.contracts import Queueable, ShouldQueue, UniqueJob
from cara.queues.delivery._DeliveryConfiguration import _replay_unique_key
from tests.queues.test_queue_operator_commands import (
    _delivery,
    _driver_application,
    _store,
)


class Hunt5ContractDriftedJob(ShouldQueue, Queueable, UniqueJob):
    """Became unique and gained a required argument after the row was signed."""

    def __init__(self, channel_id: int, marketplace: str):
        super().__init__()
        self.channel_id = channel_id
        self.marketplace = marketplace

    def unique_id(self) -> str:
        return f"scheduled-read:{self.marketplace}:{self.channel_id}"

    async def handle(self):
        return None


_DRIFTED_PAYLOAD = {
    "unique_key": None,
    "job": {
        "module": __name__,
        "class": "Hunt5ContractDriftedJob",
        "kwargs": {"channel_id": 7},
    },
}


def _store_stub() -> SimpleNamespace:
    return SimpleNamespace(options={"allowed_job_prefixes": [__name__]})


def test_a_row_the_class_can_no_longer_build_is_a_queue_refusal() -> None:
    with pytest.raises(QueueException):
        _replay_unique_key(_store_stub(), _DRIFTED_PAYLOAD)


def test_one_such_row_does_not_abort_a_queue_retry_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = pendulum.now("UTC")
    deliveries = [
        _delivery(
            "job-drifted",
            db_job_id=1,
            queue="alpha",
            status="dead_lettered",
            updated_at=now.subtract(hours=1),
        ),
        _delivery(
            "job-fine", db_job_id=2, queue="alpha", status="dead_lettered", updated_at=now
        ),
    ]
    store, _db = _store(monkeypatch, deliveries=deliveries)
    replayed: list[str] = []

    def _replay(job_id: str, *, operator: str, reason: str) -> str:
        if job_id == "job-drifted":
            return str(_replay_unique_key(_store_stub(), _DRIFTED_PAYLOAD))
        replayed.append(job_id)
        return f"replay-{job_id}"

    driver = SimpleNamespace(replay_delivery=_replay)
    command = QueueRetryCommand(application=_driver_application(driver))

    assert command.handle(all=True, store=store) == 0
    assert replayed == ["job-fine"]
