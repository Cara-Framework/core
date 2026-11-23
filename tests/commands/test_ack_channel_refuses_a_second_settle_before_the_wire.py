"""A second ACK/NACK of one delivery never reaches the broker.

Live defect: ``ThreadSafeAMQPAckChannel`` checked "already settled" AFTER
sending the frame, so the duplicate ACK/NACK was on the wire when it raised.
RabbitMQ answers a second settle of a delivery tag with a channel-level
PRECONDITION_FAILED, which closes the consumer's channel and returns every
unacknowledged job on it. Before the fix (``git show
HEAD:cara/commands/core/ThreadSafeAMQPAckChannel.py``) the wire below held
``[("ack", 7), ("nack", 7, True)]``.

The fake connection runs the scheduled callback inline, as pika's I/O thread
would; no broker, no threads to wait on.
"""

from __future__ import annotations

import pytest

from cara.commands.core.ThreadSafeAMQPAckChannel import ThreadSafeAMQPAckChannel


class _Connection:
    is_closed = False

    def add_callback_threadsafe(self, callback) -> None:
        callback()


class _Channel:
    def __init__(self, *, fail_first_ack: bool = False) -> None:
        self.wire: list[tuple] = []
        self._fail_next = fail_first_ack

    def basic_ack(self, delivery_tag) -> None:
        if self._fail_next:
            self._fail_next = False
            raise ConnectionError("stream lost")
        self.wire.append(("ack", delivery_tag))

    def basic_nack(self, delivery_tag, requeue) -> None:
        self.wire.append(("nack", delivery_tag, requeue))


def test_the_second_settle_is_refused_and_never_sent() -> None:
    channel = _Channel()
    settled: list[int] = []
    ack = ThreadSafeAMQPAckChannel(
        _Connection(), channel, on_settled=lambda: settled.append(1)
    )

    ack.basic_ack(delivery_tag=7)
    with pytest.raises(RuntimeError, match="settled more than once"):
        ack.basic_nack(delivery_tag=7, requeue=True)

    assert channel.wire == [("ack", 7)]
    assert settled == [1]


def test_a_frame_that_failed_to_send_does_not_count_as_settled() -> None:
    channel = _Channel(fail_first_ack=True)
    ack = ThreadSafeAMQPAckChannel(_Connection(), channel)

    with pytest.raises(ConnectionError):
        ack.basic_ack(delivery_tag=9)
    ack.basic_ack(delivery_tag=9)

    assert channel.wire == [("ack", 9)]
