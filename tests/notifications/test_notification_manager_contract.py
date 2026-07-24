from __future__ import annotations

import importlib

import pytest

from cara.exceptions import (
    CaraException,
    DriverNotRegisteredException,
    InvalidArgumentException,
)
from cara.notifications import BaseNotification, Notifiable
from cara.notifications.channels import BaseChannel

notification_module = importlib.import_module("cara.notifications.Notification")
NotificationManager = notification_module.Notification


class _Recipient(Notifiable):
    id = 7


class _Notification(BaseNotification):
    def __init__(self, channels):
        super().__init__()
        self.channels = channels

    def via(self, notifiable):
        return self.channels


class _Channel(BaseChannel):
    def __init__(self):
        self.calls = []

    def send(self, notifiable, notification) -> bool:
        self.calls.append((notifiable, notification))
        return True


class _RenderedNotification(_Notification):
    def to_slack(self, notifiable):
        return {"text": "Hello"}


class _RenderingChannel(BaseChannel):
    def send(self, notifiable, notification) -> bool:
        return notification.to_slack(notifiable) == {"text": "Hello"}


def test_requested_channel_must_be_registered() -> None:
    manager = NotificationManager()

    with pytest.raises(DriverNotRegisteredException):
        manager.send_now(_Recipient(), _Notification(["missing"]))


def test_channel_registration_rejects_invalid_or_duplicate_entries() -> None:
    manager = NotificationManager()
    channel = _Channel()
    manager.add_channel("database", channel)

    with pytest.raises(InvalidArgumentException, match="already registered"):
        manager.add_channel("database", _Channel())
    with pytest.raises(InvalidArgumentException, match="non-empty"):
        manager.add_channel(" ", _Channel())
    with pytest.raises(InvalidArgumentException, match=r"send\(\)"):
        manager.add_channel("broken", object())


def test_via_contract_is_explicit_and_duplicate_channels_send_once() -> None:
    manager = NotificationManager()
    channel = _Channel()
    manager.add_channel("database", channel)

    assert manager.send_now(
        _Recipient(),
        _Notification(["database", "database"]),
    )
    assert len(channel.calls) == 1

    for invalid in ("database", {"database"}, [""]):
        with pytest.raises(InvalidArgumentException):
            manager.send_now(_Recipient(), _Notification(invalid))


def test_channel_receives_the_original_notification() -> None:
    manager = NotificationManager()
    manager.add_channel("slack", _RenderingChannel())

    assert manager.send_now(_Recipient(), _RenderedNotification(["slack"]))


def test_queue_dispatch_failures_propagate(monkeypatch) -> None:
    class _QueuedNotification(_Notification, notification_module.ShouldQueue):
        pass

    def fail_dispatch(job):
        raise RuntimeError("broker unavailable")

    class _Queue:
        dispatch = staticmethod(fail_dispatch)

    monkeypatch.setattr(notification_module, "Queue", _Queue)

    with pytest.raises(RuntimeError, match="broker unavailable"):
        NotificationManager().send(
            _Recipient(),
            _QueuedNotification([]),
        )


def test_send_delayed_always_queues_even_without_should_queue(monkeypatch) -> None:
    dispatched = []

    class _Queue:
        dispatch_after = staticmethod(
            lambda job, seconds: dispatched.append((job, seconds))
        )

    monkeypatch.setattr(notification_module, "Queue", _Queue)
    notification = _Notification([])

    assert NotificationManager().send_delayed(
        _Recipient(),
        notification,
        45,
    )
    assert dispatched[0][1] == 45
    assert notification.get_delay() == 45

    with pytest.raises(InvalidArgumentException):
        NotificationManager().send_delayed(_Recipient(), notification, 0)


def test_notifiable_reads_propagate_storage_failures(monkeypatch) -> None:
    class _BrokenChannel:
        @staticmethod
        def get_notifications(recipient, read=None):
            raise RuntimeError("database unavailable")

    monkeypatch.setattr(
        _Recipient,
        "_database_notification_channel",
        staticmethod(lambda: _BrokenChannel()),
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        _Recipient().notifications()


def test_notifiable_requires_a_stable_key() -> None:
    class _Anonymous(Notifiable):
        pass

    with pytest.raises(CaraException, match="stable id or pk"):
        _Anonymous().get_notification_key()


def test_notification_fluent_metadata_rejects_ambiguous_values() -> None:
    notification = _Notification([])

    for value in ("", "   "):
        with pytest.raises(InvalidArgumentException):
            notification.id(value)
        with pytest.raises(InvalidArgumentException):
            notification.on_queue(value)
    for value in (0, -1, True, 1.5):
        with pytest.raises(InvalidArgumentException):
            notification.delay(value)
    with pytest.raises(InvalidArgumentException):
        notification.with_data([("key", "value")])
