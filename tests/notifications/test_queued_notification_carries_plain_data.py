"""A queued notification crosses the broker as plain data, one job per channel.

Live defect: ``SendNotificationJob`` carried the live notifiable and
notification. The signed JSON envelope refuses anything but JSON
primitives by design, so NO ``ShouldQueue`` notification was ever
dispatched. And one job carried every channel, so a retry after one channel
failed re-sent the channels that had already delivered. Now
``SendNotificationJob.payload_of`` turns each side into ``(class path,
plain data)`` — the notifiable as its ``{"key"}``, reloaded with ``find`` —
refusing at dispatch, naming the field, what cannot be expressed that way;
the manager queues one job PER CHANNEL; the worker rebuilds both and
delivers on its own channel only.
"""

from __future__ import annotations

import importlib

import pytest

from cara.configuration import Configuration
from cara.exceptions import CaraException, InvalidArgumentException
from cara.notifications import BaseNotification, Notifiable
from cara.notifications.channels import BaseChannel
from cara.notifications.jobs import SendNotificationJob
from cara.queues import instantiate_job
from cara.queues.contracts import ShouldQueue
from cara.queues.serializers import SignedJsonJobSerializer

manager_module = importlib.import_module("cara.notifications.Notification")
job_module = importlib.import_module("cara.notifications.jobs.SendNotificationJob")
NotificationManager = manager_module.Notification

_KEYS = {"current": "queued-notification-key-" * 3}


class Customer(Notifiable):
    rows = {7: "ada@example.com"}

    def __init__(self, id: int) -> None:
        self.id = id
        self.email = self.rows[id]

    @classmethod
    def find(cls, key):
        return cls(key) if key in cls.rows else None


class OrderShipped(BaseNotification, ShouldQueue):
    def __init__(self, order_id: int, channels=("mail", "database")) -> None:
        super().__init__()
        self.order_id = order_id
        self.channels = list(channels)

    def via(self, notifiable):
        return self.channels


class _Live:
    pass


class _Channel(BaseChannel):
    def send(self, notifiable, notification) -> bool:
        return True


@pytest.fixture(autouse=True)
def product_configuration(monkeypatch):
    """A product trusts its own package for queued classes; here, this one."""
    monkeypatch.setattr(Configuration, "_instance", None)
    Configuration.empty().set(
        "queue.job_allowed_prefixes", ["app.jobs", "tests.notifications"]
    )


def test_payload_of_is_a_class_path_and_plain_data() -> None:
    assert SendNotificationJob.payload_of(Customer(7), "notifiable") == (
        f"{__name__}.Customer",
        {"key": 7},
    )
    path, data = SendNotificationJob.payload_of(OrderShipped(42), "notification")
    assert path == f"{__name__}.OrderShipped"
    assert data["attributes"] == {"order_id": 42, "channels": ["mail", "database"]}


def test_a_live_object_on_the_notification_is_refused_naming_the_field() -> None:
    notification = OrderShipped(42)
    notification.order = _Live()

    with pytest.raises(
        InvalidArgumentException, match=r"notification_data\.attributes\.order"
    ):
        SendNotificationJob.payload_of(notification, "notification")


class NoFind(Notifiable):
    id = 1


def test_a_notifiable_without_find_is_refused_at_dispatch() -> None:
    with pytest.raises(InvalidArgumentException, match=r"has no find\(key\)"):
        SendNotificationJob.payload_of(NoFind(), "notifiable")


def test_the_manager_queues_one_job_per_channel(monkeypatch) -> None:
    dispatched: list[SendNotificationJob] = []
    monkeypatch.setattr(
        manager_module,
        "Queue",
        type("Q", (), {"dispatch": staticmethod(dispatched.append)}),
    )
    manager = NotificationManager()
    manager.add_channel("mail", _Channel())
    manager.add_channel("database", _Channel())

    assert manager.send(Customer(7), OrderShipped(42).on_queue("notify")) is True

    assert [job.channels for job in dispatched] == [["mail"], ["database"]]
    assert {job.queue for job in dispatched} == {"notify"}
    assert all(job.notifiable_data == {"key": 7} for job in dispatched)


def _round_trip(job: SendNotificationJob) -> SendNotificationJob:
    body = SignedJsonJobSerializer.serialize(
        {
            "obj": job,
            "args": (),
            "callback": "handle",
            "created": "2026-11-23T00:00:00Z",
            "job_id": "11111111-1111-4111-8111-111111111111",
            "db_job_id": 1,
            "timeout_seconds": 300,
            "_tenant": 7,
            "_tenant_mode": "tenant",
            "queue": job.queue or "notifications",
            "priority": "default",
            "dispatched_at": "2026-11-23T00:00:00Z",
        },
        signing_key_id="current",
        signing_keys=_KEYS,
        issued_at=1_763_856_000,
    )
    decoded = SignedJsonJobSerializer.deserialize(
        body, signing_keys=_KEYS, now=1_763_856_001
    )
    return instantiate_job(object(), decoded["obj"], (), decoded["init_kwargs"])


def _job(channel: str) -> SendNotificationJob:
    notifiable_class, notifiable_data = SendNotificationJob.payload_of(
        Customer(7), "notifiable"
    )
    notification_class, notification_data = SendNotificationJob.payload_of(
        OrderShipped(42), "notification"
    )
    return SendNotificationJob(
        notifiable_class,
        notifiable_data,
        notification_class,
        notification_data,
        [channel],
    )


def _facade(monkeypatch, answer):
    calls: list[tuple] = []

    class _Notification:
        NOTHING_DUE = NotificationManager.NOTHING_DUE

        @staticmethod
        def _send_now(notifiable, notification, *, only_channels):
            calls.append((notifiable, notification, only_channels))
            return answer

    monkeypatch.setattr(job_module, "Notification", _Notification)
    return calls


@pytest.mark.asyncio
async def test_the_job_survives_the_signed_envelope_and_delivers_its_channel_only(
    monkeypatch,
) -> None:
    calls = _facade(monkeypatch, True)

    await _round_trip(_job("mail")).handle()

    ((notifiable, notification, only_channels),) = calls
    assert type(notifiable) is Customer and notifiable.email == "ada@example.com"
    assert type(notification) is OrderShipped
    assert notification.order_id == 42
    assert only_channels == ["mail"]


@pytest.mark.asyncio
async def test_nothing_due_is_done_and_false_is_not_retried(monkeypatch) -> None:
    _facade(monkeypatch, NotificationManager.NOTHING_DUE)
    assert await _job("mail").handle() is None

    _facade(monkeypatch, False)
    with pytest.raises(CaraException) as raised:
        await _job("mail").handle()
    assert raised.value.do_not_retry is True


@pytest.mark.asyncio
async def test_a_deleted_notifiable_is_refused_without_retry(monkeypatch) -> None:
    _facade(monkeypatch, True)
    job = _job("mail")
    job.notifiable_data = {"key": 999}

    with pytest.raises(CaraException, match="no longer exists") as raised:
        await job.handle()
    assert raised.value.do_not_retry is True
