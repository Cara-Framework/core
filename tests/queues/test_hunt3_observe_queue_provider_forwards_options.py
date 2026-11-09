"""The relay must run on the window the product declares.

``QueueJobDeliveryStore`` reads the broker window, the tenant window, the urgent
slots and the aging interval from its driver's options, and every product
declares them in ``config/queue.py``. ``QueueProvider`` copies the driver's
options key by key, and those four keys are not among them, so the store falls
back to its own defaults: window 2, no tenant window, no urgent slots.

Every relay test builds the store with these options by hand, so none of them
sees the gap. A scratch relay with ``QUEUE_DELIVERY_BROKER_WINDOW_PER_QUEUE=4``
kept two rows published per queue, against four-slot pools (2026-11-09).
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

_provider = importlib.import_module("cara.queues.QueueProvider")

_DECLARED = {
    "delivery_broker_window_per_queue": 4,
    "delivery_tenant_window_per_queue": 3,
    "delivery_tenant_urgent_slots_per_queue": 1,
    "delivery_priority_aging_seconds": 600,
}


@pytest.fixture
def forwarded(monkeypatch):
    captured = {}

    class _Driver:
        driver_name = "amqp"

        def __init__(self, application, options):
            captured.update(options)

    values = {
        "queue.drivers.amqp": {"username": "u"},
        "queue.drivers.amqp.username": "u",
        "queue.drivers.amqp.password": "p",
        "queue.canonical_queues": ("observe",),
        **{f"queue.drivers.amqp.{key}": value for key, value in _DECLARED.items()},
    }
    monkeypatch.setattr(
        _provider, "config", lambda key, default=None: values.get(key, default)
    )
    monkeypatch.setattr(_provider, "AMQPDriver", _Driver)
    provider = _provider.QueueProvider.__new__(_provider.QueueProvider)
    provider.application = object()
    provider._add_amqp_driver(SimpleNamespace(add_driver=lambda *_args: None))
    return captured


@pytest.mark.parametrize("key", sorted(_DECLARED))
def test_a_declared_relay_option_reaches_the_driver(forwarded, key):
    assert forwarded.get(key) == _DECLARED[key]
