"""The dead-letter peek must be a read: distinct messages, no delivery cost.

The fake channel models the RabbitMQ 4.3 quorum-queue rules the peek relies
on: a returned message goes back to the HEAD of the queue, a nack counts
toward ``acquired-count`` only, and closing a channel that still holds
messages counts each as a failed delivery toward ``x-delivery-limit``.
"""

from __future__ import annotations

from types import SimpleNamespace

from cara.queues.drivers import _AMQPBroker


class _QuorumChannel:
    def __init__(self, bodies: list[bytes]):
        self.ready = [{"body": body, "delivery_count": 0} for body in bodies]
        self.unacked: dict[int, dict] = {}
        self.next_tag = 1
        self.nacks: list[dict] = []

    def basic_get(self, _queue, auto_ack):
        assert auto_ack is False
        if not self.ready:
            return None, None, None
        message = self.ready.pop(0)
        tag = self.next_tag
        self.next_tag += 1
        self.unacked[tag] = message
        method = SimpleNamespace(
            delivery_tag=tag,
            routing_key="dead.sync",
            redelivered=False,
            exchange="dlx",
        )
        properties = SimpleNamespace(headers={}, priority=0, timestamp=None)
        return method, properties, message["body"]

    def basic_nack(self, delivery_tag, multiple=False, requeue=True):
        self.nacks.append({"tag": delivery_tag, "multiple": multiple})
        assert requeue is True
        tags = sorted(
            tag
            for tag in self.unacked
            if tag == delivery_tag or (multiple and tag <= delivery_tag)
        )
        self.ready[:0] = [self.unacked.pop(tag) for tag in tags]

    def close(self):
        for tag in sorted(self.unacked):
            message = self.unacked.pop(tag)
            message["delivery_count"] += 1
            self.ready.insert(0, message)


def _driver(channel: _QuorumChannel) -> SimpleNamespace:
    driver = SimpleNamespace(
        options={"signing_keys": {}},
        channel=None,
        connection=None,
    )

    def connect(_options):
        driver.channel = channel
        driver.connection = SimpleNamespace(close=lambda: None)

    driver._connect = connect
    return driver


def _peek(channel: _QuorumChannel, limit: int) -> list[str]:
    messages = _AMQPBroker._amqp_broker_get_dead_letter_messages(
        _driver(channel),
        limit=limit,
    )
    return [message["payload"]["raw"] for message in messages]


def test_one_pass_reads_each_message_once_in_queue_order():
    channel = _QuorumChannel([b"first", b"second", b"third"])

    assert _peek(channel, limit=10) == ["first", "second", "third"]
    assert channel.nacks == [{"tag": 3, "multiple": True}]
    assert channel.unacked == {}


def test_repeated_peeks_leave_the_dead_letter_queue_untouched():
    channel = _QuorumChannel([b"first", b"second", b"third"])

    for _ in range(30):
        assert _peek(channel, limit=2) == ["first", "second"]

    assert [message["body"] for message in channel.ready] == [
        b"first",
        b"second",
        b"third",
    ]
    assert all(message["delivery_count"] == 0 for message in channel.ready)


def test_an_empty_queue_sends_no_nack():
    channel = _QuorumChannel([])

    assert _peek(channel, limit=5) == []
    assert channel.nacks == []
