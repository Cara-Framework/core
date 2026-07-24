"""Cara notification primitives must not encode a product's schema or queue."""

from __future__ import annotations

import json

from cara.notifications import BaseNotification, Notifiable
from cara.notifications.channels import DatabaseChannel
from cara.notifications.jobs import SendNotificationJob


class _Recipient(Notifiable):
    id = 41


class _DatabaseNotification(BaseNotification):
    def via(self, notifiable) -> list[str]:
        return ["database"]

    def to_database(self, notifiable) -> dict[str, str]:
        return {"message": "Ready"}


class _Table:
    def __init__(self) -> None:
        self.created = []

    def create(self, values):
        self.created.append(values)
        return True


class _Database:
    def __init__(self, table: _Table) -> None:
        self._table = table

    def table(self, name: str) -> _Table:
        assert name == "notifications"
        return self._table


def test_database_channel_uses_generic_polymorphic_schema():
    table = _Table()
    channel = DatabaseChannel(_Database(table))

    assert channel.send(_Recipient(), _DatabaseNotification()) is True

    record = table.created[0]
    assert record["notifiable_type"] == "_Recipient"
    assert record["notifiable_id"] == 41
    assert json.loads(record["data"]) == {"message": "Ready"}
    assert "tenant_id" not in record
    assert "user_id" not in record
    assert "status" not in record


def test_notifiable_reads_and_mutations_delegate_to_the_database_channel(monkeypatch):
    calls = []

    class _Channel:
        def get_notifications(self, recipient, read=None):
            calls.append(("get", recipient.id, read))
            return [{"read": read}]

        def mark_as_read(self, recipient, ids):
            calls.append(("read", recipient.id, ids))
            return True

        def mark_as_unread(self, recipient, ids):
            calls.append(("unread", recipient.id, ids))
            return True

    monkeypatch.setattr(
        _Recipient,
        "_database_notification_channel",
        staticmethod(lambda: _Channel()),
    )
    recipient = _Recipient()

    assert recipient.notifications() == [{"read": None}]
    assert recipient.unread_notifications() == [{"read": False}]
    assert recipient.read_notifications() == [{"read": True}]
    recipient.mark_as_read(["one"])
    recipient.mark_as_unread(["two"])

    assert calls == [
        ("get", 41, None),
        ("get", 41, False),
        ("get", 41, True),
        ("read", 41, ["one"]),
        ("unread", 41, ["two"]),
    ]


def test_queued_notifications_do_not_invent_a_product_queue():
    assert SendNotificationJob.default_queue is None
