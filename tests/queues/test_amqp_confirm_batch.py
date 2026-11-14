"""A relay batch is published once and its confirms are awaited ONCE.

Sync confirms cost one quorum-queue round trip per row: a 64-row batch took
21-45 ms one by one and 1.8 ms published together (private RabbitMQ 4.2,
canonical quorum arguments, 2026-11-13). ``AMQPConfirmBatch`` maps every
``Basic.Ack``/``Basic.Nack`` — ``multiple`` included — and every
``Basic.Return`` back to its message, and wakes the blocked I/O wait the
moment the batch is settled instead of after a fixed tick.
"""

from __future__ import annotations

from types import SimpleNamespace

import pika
import pytest

from cara.exceptions import QueueException
from cara.queues.drivers import AMQPConfirmBatch


class _Impl:
    """The underlying ``pika.channel.Channel``: records publishes, holds callbacks."""

    def __init__(self):
        self.is_closed = False
        self.published: list[dict] = []
        self.on_confirm = None
        self.on_select_ok = None
        self.on_return = None
        self.on_close = None

    def confirm_delivery(self, *, ack_nack_callback, callback):
        self.on_confirm = ack_nack_callback
        self.on_select_ok = callback

    def add_on_return_callback(self, callback):
        self.on_return = callback

    def add_on_close_callback(self, callback):
        self.on_close = callback

    def basic_publish(self, **kwargs):
        self.published.append(kwargs)


class _Connection:
    """A blocking connection whose broker answers from a script on each poll."""

    def __init__(self, broker):
        self.impl = _Impl()
        self.broker = broker
        self.wakes = 0
        self.polls = 0
        self.timers: list = []

    def channel(self):
        return SimpleNamespace(_impl=self.impl, is_open=True, close=lambda: None)

    def call_later(self, _delay, callback):
        self.timers.append(callback)
        return callback

    def remove_timeout(self, timer):
        self.timers.remove(timer)

    def add_callback_threadsafe(self, _callback):
        self.wakes += 1

    def process_data_events(self, time_limit=0):
        # Blocking waits only: a fixed tick would hold every batch for it.
        assert time_limit is None
        self.polls += 1
        self.broker(self)


def _ack(tag, *, multiple=False, nack=False):
    method = (pika.spec.Basic.Nack if nack else pika.spec.Basic.Ack)(
        delivery_tag=tag, multiple=multiple
    )
    return SimpleNamespace(method=method)


def _messages(count):
    return [
        ("", "sync", b"{}", pika.BasicProperties(message_id=f"job-{index}"))
        for index in range(count)
    ]


def _select_ok_then(script):
    """Answer Confirm.Select first, then run ``script`` on later polls."""

    def broker(connection):
        if connection.polls == 1:
            connection.impl.on_select_ok(None)
            return
        script(connection)

    return broker


def test_one_multiple_ack_settles_the_whole_batch_in_one_wait():
    connection = _Connection(
        _select_ok_then(lambda c: c.impl.on_confirm(_ack(5, multiple=True)))
    )
    batch = AMQPConfirmBatch(connection)

    outcomes = batch.publish(_messages(5))

    assert outcomes == [None] * 5
    assert [m["mandatory"] for m in connection.impl.published] == [True] * 5
    # Select-ok, then ONE wait for five confirms.
    assert connection.polls == 2
    assert connection.wakes == 2
    assert connection.timers == []


def test_a_nack_and_a_return_are_reported_for_their_own_message():
    def broker(connection):
        impl = connection.impl
        impl.on_return(
            None,
            SimpleNamespace(reply_code=312, reply_text="NO_ROUTE"),
            pika.BasicProperties(message_id="job-1"),
            b"{}",
        )
        impl.on_confirm(_ack(1))
        impl.on_confirm(_ack(2))
        impl.on_confirm(_ack(3, nack=True))

    batch = AMQPConfirmBatch(_Connection(_select_ok_then(broker)))

    outcomes = batch.publish(_messages(3))

    assert outcomes[0] is None
    assert isinstance(outcomes[1], pika.exceptions.UnroutableError)
    assert isinstance(outcomes[2], pika.exceptions.NackError)


def test_tags_continue_across_batches_on_one_channel():
    confirms = iter([_ack(2, multiple=True), _ack(3)])
    connection = _Connection(_select_ok_then(lambda c: c.impl.on_confirm(next(confirms))))
    batch = AMQPConfirmBatch(connection)

    assert batch.publish(_messages(2)) == [None, None]
    assert batch.publish(_messages(1)) == [None]


def test_a_silent_broker_fails_the_batch_at_the_deadline():
    def broker(connection):
        # Nothing confirms; the deadline timer is the event that wakes us.
        for timer in list(connection.timers):
            timer()

    batch = AMQPConfirmBatch(_Connection(_select_ok_then(broker)))

    with pytest.raises(QueueException, match="did not confirm"):
        batch.publish(_messages(2))


def test_a_closed_channel_fails_the_batch_instead_of_waiting():
    def broker(connection):
        connection.impl.is_closed = True
        connection.impl.on_close(None, None)

    batch = AMQPConfirmBatch(_Connection(_select_ok_then(broker)))

    with pytest.raises(pika.exceptions.ChannelClosed):
        batch.publish(_messages(1))
