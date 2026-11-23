"""A send with nothing due is not a failure; an outbox fan-out is atomic.

Live defects in ``cara/notifications/Notification.py``:

1. ``_send_now`` returned ``all(results)`` over an EMPTY list when
   ``should_send`` skipped every channel — i.e. ``True`` for nothing sent —
   and later ``False``, which the queued job read as "a channel failed" and
   retried a notification that was deliberately skipped. It now answers
   ``Notification.NOTHING_DUE``: falsy, but distinguishable by identity.
2. A queued notification is one job per channel. With a transactional-outbox
   driver each push wrote its own outbox row, so a failure on the third
   channel left the first two queued behind an exception inviting the caller
   to notify again. The fan-out is now written inside one ``DB.transaction()``.
"""

from __future__ import annotations

import importlib
from contextlib import contextmanager

import pytest

from cara.configuration import Configuration
from cara.notifications import BaseNotification, Notifiable
from cara.notifications.channels import BaseChannel
from cara.queues.contracts import ShouldQueue

notification_module = importlib.import_module("cara.notifications.Notification")
NotificationManager = notification_module.Notification


class _Recipient(Notifiable):
    id = 3

    @classmethod
    def find(cls, key):
        return cls() if key == cls.id else None


class _Notice(BaseNotification):
    def __init__(self, channels, skip=()):
        super().__init__()
        self.channels = list(channels)
        self.skip = list(skip)

    def via(self, notifiable):
        return self.channels

    def should_send(self, notifiable, channel):
        return channel not in self.skip


class _QueuedNotice(_Notice, ShouldQueue):
    pass


class _Channel(BaseChannel):
    def __init__(self, answer=True):
        self.answer = answer
        self.calls = 0

    def send(self, notifiable, notification) -> bool:
        self.calls += 1
        return self.answer


def _manager(**channels):
    manager = NotificationManager()
    for name, channel in channels.items():
        manager.add_channel(name, channel)
    return manager


def test_every_channel_skipped_answers_nothing_due() -> None:
    mail, slack = _Channel(), _Channel()
    manager = _manager(mail=mail, slack=slack)

    result = manager.send_now(
        _Recipient(), _Notice(["mail", "slack"], skip=["mail", "slack"])
    )

    assert result is NotificationManager.NOTHING_DUE
    assert not result
    assert result is not False
    assert mail.calls == slack.calls == 0


def test_a_normal_send_still_answers_a_bool() -> None:
    assert _manager(mail=_Channel()).send_now(_Recipient(), _Notice(["mail"])) is True
    assert (
        _manager(mail=_Channel(), slack=_Channel(answer=False)).send_now(
            _Recipient(), _Notice(["mail", "slack"])
        )
        is False
    )


def test_one_channel_skipped_one_sent_is_true_not_nothing_due() -> None:
    result = _manager(mail=_Channel(), slack=_Channel()).send_now(
        _Recipient(), _Notice(["mail", "slack"], skip=["mail"])
    )
    assert result is True


class _Recorder:
    def __init__(self, *, outbox: bool, fail_on: int | None = None):
        self.events: list[str] = []
        self.outbox = outbox
        self.fail_on = fail_on
        self.pushed = 0

    def queue_facade(self):
        recorder = self

        class _Driver:
            durable_transactional_outbox = recorder.outbox

        class _Queue:
            @staticmethod
            def driver():
                return _Driver()

            @staticmethod
            def dispatch(job):
                recorder.pushed += 1
                if recorder.fail_on == recorder.pushed:
                    raise RuntimeError("outbox write failed")
                recorder.events.append(f"push:{job.channels[0]}")

        return _Queue

    def db_facade(self):
        recorder = self

        class _DB:
            @staticmethod
            @contextmanager
            def transaction():
                recorder.events.append("begin")
                try:
                    yield
                except BaseException:
                    recorder.events.append("rollback")
                    raise
                recorder.events.append("commit")

        return _DB


@pytest.fixture
def queued(monkeypatch):
    monkeypatch.setattr(Configuration, "_instance", None)
    Configuration.empty().set(
        "queue.job_allowed_prefixes", ["app.jobs", "tests.notifications"]
    )

    def install(recorder: _Recorder) -> None:
        monkeypatch.setattr(notification_module, "Queue", recorder.queue_facade())
        monkeypatch.setattr(notification_module, "DB", recorder.db_facade())

    return install


def test_outbox_fanout_is_written_in_one_transaction(queued) -> None:
    recorder = _Recorder(outbox=True)
    queued(recorder)
    manager = _manager(mail=_Channel(), slack=_Channel(), database=_Channel())

    assert manager.send(_Recipient(), _QueuedNotice(["mail", "slack", "database"]))

    assert recorder.events == [
        "begin",
        "push:mail",
        "push:slack",
        "push:database",
        "commit",
    ]


def test_outbox_fanout_failure_rolls_back_the_earlier_channels(queued) -> None:
    recorder = _Recorder(outbox=True, fail_on=3)
    queued(recorder)
    manager = _manager(mail=_Channel(), slack=_Channel(), database=_Channel())

    with pytest.raises(RuntimeError, match="outbox write failed"):
        manager.send(_Recipient(), _QueuedNotice(["mail", "slack", "database"]))

    assert recorder.events == ["begin", "push:mail", "push:slack", "rollback"]


def test_a_non_outbox_driver_pushes_without_a_transaction(queued) -> None:
    recorder = _Recorder(outbox=False)
    queued(recorder)
    manager = _manager(mail=_Channel(), slack=_Channel())

    assert manager.send(_Recipient(), _QueuedNotice(["mail", "slack"]))

    assert recorder.events == ["push:mail", "push:slack"]
