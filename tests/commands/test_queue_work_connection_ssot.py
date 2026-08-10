"""``queue:work`` AMQP connections come from ONE builder — never a local copy.

``AMQPConnectionManager._create_connection`` used to carry an ``else``
branch that restated ``AMQPDriver._connection_parameters`` by hand. The copy
knew nothing about ``scheme``: it built ``pika.ConnectionParameters`` with
``PlainCredentials`` and no ``ssl_options``, so a worker whose operator had
configured ``RABBIT_SCHEME=amqps`` would have opened a PLAINTEXT connection
and put the broker username and password on the wire. Nothing in the method
noticed; the branch was masked only by an unrelated precondition 1600 lines
away.

These tests pin the two halves of the fix: the driver's parameters are used
verbatim (TLS included), and a manager that cannot reach the driver REFUSES
rather than inventing its own downgraded parameters.
"""

from __future__ import annotations

import importlib
import ssl
from types import SimpleNamespace

import pytest

from cara.commands.core.QueueWorkCommand import AMQPConnectionManager
from cara.exceptions import ConfigurationException


def _amqp_config(key, default=None):
    """A fully-populated plaintext AMQP config — the fallback's fuel."""
    return {
        "queue.drivers.amqp.username": "broker-user",
        "queue.drivers.amqp.password": "broker-secret",
        "queue.drivers.amqp.host": "rabbit.internal",
        "queue.drivers.amqp.port": 5672,
        "queue.drivers.amqp.vhost": "/",
    }.get(key, default)


def test_tls_parameters_from_the_driver_reach_pika_untouched(monkeypatch):
    """The TLS context the driver built must survive the handoff."""
    pika = importlib.import_module("pika")
    parameters = pika.ConnectionParameters(host="rabbit.internal", port=5671)
    parameters.ssl_options = pika.SSLOptions(
        ssl.create_default_context(),
        server_hostname="rabbit.internal",
    )
    driver = SimpleNamespace(
        options={"scheme": "amqps"},
        _connection_parameters=lambda options: parameters,
    )
    captured = []
    connection = object()
    monkeypatch.setattr(
        pika,
        "BlockingConnection",
        lambda value: captured.append(value) or connection,
    )

    manager = AMQPConnectionManager(_amqp_config, driver)

    assert manager._create_connection() is connection
    assert captured == [parameters]
    assert captured[0].ssl_options is parameters.ssl_options


def test_a_driverless_manager_refuses_instead_of_connecting_in_plaintext(monkeypatch):
    """Pins the removed fail-open branch.

    Pre-fix this returned a live ``BlockingConnection`` built from
    ``PlainCredentials`` over an unencrypted socket, regardless of the
    configured scheme. The only correct answer to "I cannot ask the driver
    how to connect" is to refuse.
    """
    pika = importlib.import_module("pika")
    attempts = []
    monkeypatch.setattr(
        pika,
        "BlockingConnection",
        lambda value: attempts.append(value) or object(),
    )

    manager = AMQPConnectionManager(_amqp_config, driver=None)

    with pytest.raises(ConfigurationException, match="requires the AMQP driver"):
        manager._create_connection()
    assert attempts == []


def test_a_driver_that_cannot_describe_its_connection_is_refused(monkeypatch):
    """A driver without ``_connection_parameters`` is a misconfiguration."""
    pika = importlib.import_module("pika")
    attempts = []
    monkeypatch.setattr(
        pika,
        "BlockingConnection",
        lambda value: attempts.append(value) or object(),
    )

    manager = AMQPConnectionManager(_amqp_config, SimpleNamespace(options={}))

    with pytest.raises(ConfigurationException):
        manager._create_connection()
    assert attempts == []


def test_refusal_is_fail_closed_not_a_silent_downgrade(monkeypatch):
    """``ensure_connection`` reports failure and holds no connection."""
    pika = importlib.import_module("pika")
    monkeypatch.setattr(pika, "BlockingConnection", lambda value: object())

    manager = AMQPConnectionManager(_amqp_config, driver=None)

    assert manager.ensure_connection() is False
    assert manager.connection is None
