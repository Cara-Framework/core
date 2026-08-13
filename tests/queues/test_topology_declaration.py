"""Broker topology declaration: the 406-replace protocol and the DLQ trio.

Two behaviours have to hold no matter which application drives the reconcile:

* a queue whose arguments disagree with the driver's contract is replaced ONLY
  when it is provably empty and unconsumed — otherwise the reconcile refuses
  rather than discarding queued work;
* the dead-letter binding is ``dead.#``. The driver's dead-letter routing key
  is ``dead.<queue name>`` and canonical queue names contain dots, so the
  single-word ``dead.*`` this used to be silently dropped every dead-letter for
  a dotted queue.
"""

from __future__ import annotations

from types import SimpleNamespace

import pika
import pytest

from cara.exceptions import QueueException
from cara.queues.QueueState import (
    DEAD_LETTER_BINDING,
    DEAD_LETTER_EXCHANGE,
    DEAD_LETTER_QUEUE,
    QueueState,
    declare_dead_letter_topology,
    ensure_exact_queue,
    format_queue_states,
    inspect_queue,
)


class _FakeChannel:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection
        self.is_open = True

    def queue_declare(self, *, queue: str, passive: bool = False, **kwargs):
        state = self._connection.queues.get(queue)
        if passive:
            if state is None:
                self.is_open = False
                raise pika.exceptions.ChannelClosedByBroker(404, "NOT_FOUND")
            return SimpleNamespace(
                method=SimpleNamespace(
                    message_count=state["messages"],
                    consumer_count=state["consumers"],
                )
            )
        self._connection.declares.append((queue, kwargs.get("arguments")))
        if state is not None and state["arguments"] != kwargs.get("arguments"):
            self.is_open = False
            raise pika.exceptions.ChannelClosedByBroker(406, "PRECONDITION_FAILED")
        self._connection.queues[queue] = {
            "messages": 0,
            "consumers": 0,
            "arguments": kwargs.get("arguments"),
        }
        return SimpleNamespace(method=SimpleNamespace(message_count=0, consumer_count=0))

    def queue_delete(self, *, queue: str, if_unused: bool, if_empty: bool):
        state = self._connection.queues.get(queue)
        if state and (state["messages"] or state["consumers"]):
            self.is_open = False
            raise pika.exceptions.ChannelClosedByBroker(406, "PRECONDITION_FAILED")
        self._connection.queues.pop(queue, None)
        self._connection.deleted.append(queue)

    def exchange_declare(self, *, exchange: str, exchange_type: str, durable: bool):
        self._connection.exchanges.append((exchange, exchange_type, durable))

    def queue_bind(self, *, queue: str, exchange: str, routing_key: str):
        self._connection.bindings.append((queue, exchange, routing_key))

    def close(self) -> None:
        self.is_open = False


class _FakeConnection:
    def __init__(self, queues: dict | None = None) -> None:
        self.queues = queues or {}
        self.declares: list[tuple[str, dict | None]] = []
        self.deleted: list[str] = []
        self.exchanges: list[tuple[str, str, bool]] = []
        self.bindings: list[tuple[str, str, str]] = []
        self.is_open = True

    def channel(self) -> _FakeChannel:
        return _FakeChannel(self)

    def close(self) -> None:
        self.is_open = False


def _existing(arguments: dict, *, messages: int = 0, consumers: int = 0) -> dict:
    return {"messages": messages, "consumers": consumers, "arguments": arguments}


def test_inspect_queue_returns_none_for_a_missing_queue() -> None:
    assert inspect_queue(_FakeConnection(), "absent") is None


def test_inspect_queue_reports_depth_and_consumers() -> None:
    connection = _FakeConnection({"alpha": _existing({}, messages=4, consumers=2)})
    assert inspect_queue(connection, "alpha") == QueueState(
        name="alpha", messages=4, consumers=2
    )


def test_ensure_exact_queue_declares_a_missing_queue_once() -> None:
    connection = _FakeConnection()
    ensure_exact_queue(connection, "alpha", {"x-queue-type": "quorum"})
    assert connection.queues["alpha"]["arguments"] == {"x-queue-type": "quorum"}
    assert connection.deleted == []


def test_ensure_exact_queue_replaces_an_empty_incompatible_queue() -> None:
    connection = _FakeConnection({"alpha": _existing({"x-queue-type": "classic"})})
    ensure_exact_queue(connection, "alpha", {"x-queue-type": "quorum"})
    assert connection.deleted == ["alpha"]
    assert connection.queues["alpha"]["arguments"] == {"x-queue-type": "quorum"}


def test_ensure_exact_queue_refuses_to_replace_an_active_incompatible_queue() -> None:
    connection = _FakeConnection(
        {"alpha": _existing({"x-queue-type": "classic"}, messages=3)}
    )
    with pytest.raises(QueueException) as excinfo:
        ensure_exact_queue(connection, "alpha", {"x-queue-type": "quorum"})
    assert "alpha(messages=3, consumers=0)" in str(excinfo.value)
    assert connection.deleted == []


def test_declare_dead_letter_topology_uses_the_framework_names_and_hash_binding() -> None:
    connection = _FakeConnection()
    channel = connection.channel()
    driver = SimpleNamespace(
        dead_letter_queue_arguments=lambda: {"x-queue-type": "quorum"}
    )

    declare_dead_letter_topology(driver, connection, channel)

    assert connection.exchanges == [(DEAD_LETTER_EXCHANGE, "topic", True)]
    assert DEAD_LETTER_QUEUE in connection.queues
    assert connection.bindings == [
        (DEAD_LETTER_QUEUE, DEAD_LETTER_EXCHANGE, DEAD_LETTER_BINDING)
    ]


def test_dead_letter_binding_matches_a_dotted_canonical_queue_routing_key() -> None:
    # AMQPDriver.canonical_queue_arguments sets x-dead-letter-routing-key to
    # f"dead.{queue_name}". "dead.*" matches exactly one word after "dead.",
    # so a dotted queue name needs "#".
    assert DEAD_LETTER_BINDING == "dead.#"
    routing_key = "dead.connector.amazon"
    assert routing_key.startswith("dead.")
    assert len(routing_key.split(".")) > 2


def test_format_queue_states_names_what_blocked_the_operator() -> None:
    rendered = format_queue_states([QueueState(name="alpha", messages=2, consumers=1)])
    assert rendered == "alpha(messages=2, consumers=1)"
