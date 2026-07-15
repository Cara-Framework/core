from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock

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
    fake_pika = SimpleNamespace(
        URLParameters=MagicMock(return_value=SimpleNamespace()),
        BlockingConnection=MagicMock(side_effect=ConnectionRefusedError("down")),
        exceptions=SimpleNamespace(AMQPError=RuntimeError),
    )
    monkeypatch.setattr(module, "pika", fake_pika)
    driver = object.__new__(AMQPDriver)
    driver.options = {}

    with pytest.raises(ConnectionRefusedError):
        driver.ping(timeout_ms=100)
