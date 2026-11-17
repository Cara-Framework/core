from __future__ import annotations

import importlib
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pika
import pytest

from cara.queues.drivers.AMQPDriver import AMQPDriver
from cara.queues.Queue import Queue as QueueManager


def test_queue_manager_delegates_ping_to_selected_driver() -> None:
    driver = MagicMock()
    manager = QueueManager(application=None, default_driver="amqp")
    manager.add_driver("amqp", driver)

    manager.ping(timeout_ms=750)

    driver.ping.assert_called_once_with(timeout_ms=750)


def test_amqp_ping_performs_isolated_bounded_handshake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("cara.queues.drivers.AMQPDriver")
    broker = importlib.import_module("cara.queues.drivers._AMQPBroker")
    parameters = SimpleNamespace()
    channel = MagicMock()
    connection = MagicMock()
    connection.channel.return_value = channel
    fake_pika = SimpleNamespace(
        URLParameters=MagicMock(return_value=parameters),
        BlockingConnection=MagicMock(return_value=connection),
        exceptions=SimpleNamespace(AMQPError=RuntimeError),
    )
    monkeypatch.setattr(module, "pika", fake_pika)
    monkeypatch.setattr(broker, "pika", fake_pika)

    driver = object.__new__(AMQPDriver)
    driver.options = {
        "username": "user",
        "password": "pass",
        "host": "rabbit",
        "port": 5672,
        "vhost": "/",
    }

    driver.ping(timeout_ms=750)

    fake_pika.URLParameters.assert_called_once()
    fake_pika.BlockingConnection.assert_called_once_with(parameters)
    assert parameters.connection_attempts == 1
    assert parameters.retry_delay == 0
    assert parameters.socket_timeout == 0.75
    assert parameters.stack_timeout == 0.75
    assert parameters.blocked_connection_timeout == 0.75
    connection.channel.assert_called_once_with()
    channel.close.assert_called_once_with()
    connection.close.assert_called_once_with()


def test_amqp_ping_propagates_connection_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("cara.queues.drivers.AMQPDriver")
    broker = importlib.import_module("cara.queues.drivers._AMQPBroker")
    fake_pika = SimpleNamespace(
        URLParameters=MagicMock(return_value=SimpleNamespace()),
        BlockingConnection=MagicMock(side_effect=ConnectionRefusedError("down")),
        exceptions=SimpleNamespace(AMQPError=RuntimeError),
    )
    monkeypatch.setattr(module, "pika", fake_pika)
    monkeypatch.setattr(broker, "pika", fake_pika)
    driver = object.__new__(AMQPDriver)
    driver.options = {}

    with pytest.raises(ConnectionRefusedError):
        driver.ping(timeout_ms=100)


def _runtime_health_driver(access: str):
    driver = object.__new__(AMQPDriver)
    driver.options = {"broker_access": access}
    driver._canonical_queues = frozenset({"connector", "sync"})
    driver._runtime_health_cache = {}
    driver._tls = threading.local()
    driver._delivery_store = SimpleNamespace(verify_schema=MagicMock())
    bootstrap = MagicMock()
    channel = MagicMock()
    connection = MagicMock()
    connection.channel.return_value = channel
    driver.open_topology_connection = MagicMock(return_value=(connection, bootstrap))
    return driver, connection, channel


def test_consumer_health_checks_only_assigned_readable_queues() -> None:
    driver, connection, channel = _runtime_health_driver("consume")

    driver.verify_runtime_health(["sync"], force=True)

    driver._delivery_store.verify_schema.assert_called_once_with()
    channel.queue_declare.assert_called_once_with(
        queue="sync",
        passive=True,
    )
    channel.exchange_declare.assert_not_called()
    connection.close.assert_not_called()


def test_publisher_health_proves_write_permission_with_unroutable_probe() -> None:
    driver, connection, channel = _runtime_health_driver("publish")
    channel.basic_publish.side_effect = pika.exceptions.UnroutableError([])

    driver.verify_runtime_health(force=True)

    driver._delivery_store.verify_schema.assert_called_once_with()
    connection.channel.assert_called_once_with()
    channel.confirm_delivery.assert_called_once_with()
    publish = channel.basic_publish.call_args.kwargs
    assert publish["exchange"] == ""
    assert publish["routing_key"].startswith("__cara_write_probe__.")
    assert publish["body"] == b""
    assert publish["mandatory"] is True
    assert publish["properties"].type == "cara.queue.write-probe"
    channel.queue_declare.assert_not_called()
    channel.exchange_declare.assert_not_called()
    channel.close.assert_called_once_with()
    connection.close.assert_not_called()


def test_publisher_health_fails_when_default_exchange_write_is_denied() -> None:
    driver, _connection, channel = _runtime_health_driver("publish")
    channel.basic_publish.side_effect = pika.exceptions.ChannelClosedByBroker(
        403,
        "ACCESS_REFUSED",
    )

    with pytest.raises(pika.exceptions.ChannelClosedByBroker):
        driver.verify_runtime_health(force=True)


def test_db_only_health_never_attempts_broker_authentication() -> None:
    driver, _connection, _channel = _runtime_health_driver("none")

    driver.verify_runtime_health(force=True)

    driver._delivery_store.verify_schema.assert_called_once_with()
    driver.open_topology_connection.assert_not_called()


def test_health_probe_reuses_one_connection_per_thread() -> None:
    driver, connection, channel = _runtime_health_driver("consume")

    driver.verify_runtime_health(["sync"], force=True)
    driver.verify_runtime_health(["sync"], force=True)

    driver.open_topology_connection.assert_called_once_with()
    connection.process_data_events.assert_called_once_with(time_limit=0)
    assert channel.queue_declare.call_count == 2
    connection.close.assert_not_called()


def test_health_probe_reopens_a_connection_the_broker_dropped() -> None:
    driver, stale, _channel = _runtime_health_driver("consume")
    driver.verify_runtime_health(["sync"], force=True)
    fresh = MagicMock()
    fresh.channel.return_value = MagicMock()
    driver.open_topology_connection.return_value = (fresh, MagicMock())
    stale.process_data_events.side_effect = pika.exceptions.StreamLostError()

    driver.verify_runtime_health(["sync"], force=True)

    stale.close.assert_called_once_with()
    fresh.channel.return_value.queue_declare.assert_called_once_with(
        queue="sync",
        passive=True,
    )


def test_health_probe_retries_once_when_a_reused_connection_dies_mid_probe() -> None:
    driver, stale, channel = _runtime_health_driver("consume")
    driver.verify_runtime_health(["sync"], force=True)
    fresh = MagicMock()
    fresh.channel.return_value = MagicMock()
    driver.open_topology_connection.return_value = (fresh, MagicMock())

    def lost(**_kwargs):
        stale.is_open = False
        raise pika.exceptions.StreamLostError()

    channel.queue_declare.side_effect = lost

    driver.verify_runtime_health(["sync"], force=True)

    assert driver.open_topology_connection.call_count == 2
    fresh.channel.return_value.queue_declare.assert_called_once()


def test_health_probe_refusal_on_a_live_connection_fails_and_keeps_it() -> None:
    driver, connection, channel = _runtime_health_driver("consume")
    channel.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(
        404,
        "NOT_FOUND",
    )

    with pytest.raises(pika.exceptions.ChannelClosedByBroker):
        driver.verify_runtime_health(["sync"], force=True)

    driver.open_topology_connection.assert_called_once_with()
    connection.close.assert_not_called()


def test_health_probe_fresh_connection_failure_propagates() -> None:
    driver, connection, channel = _runtime_health_driver("consume")

    def lost(**_kwargs):
        connection.is_open = False
        raise pika.exceptions.StreamLostError()

    channel.queue_declare.side_effect = lost

    with pytest.raises(pika.exceptions.StreamLostError):
        driver.verify_runtime_health(["sync"], force=True)

    driver.open_topology_connection.assert_called_once_with()
    assert driver._tls.health_connection is None
