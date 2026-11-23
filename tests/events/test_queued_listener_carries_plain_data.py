"""A queued listener hop carries plain event data and fully qualified names.

Live defects in the ``ShouldQueue`` listener path (``Event._queue_listener``
/ ``HandleListenerJob``):

1. The hop carried the event's raw attributes; a live object among them
   made the signed JSON envelope refuse the whole hop with an opaque
   serializer error. ``HandleListenerJob.event_payload`` now builds the
   payload (``to_dict()`` or the public attributes) and refuses non-plain
   data at dispatch, naming the field.
2. The event class travelled by its BARE name, so two events called
   ``Placed`` in different modules rebuilt whichever registered first. The
   hop now carries the fully qualified name; a bare name is still accepted
   only when exactly one registered class carries it.
"""

from __future__ import annotations

import importlib

import pytest

from cara.configuration import Configuration
from cara.events import Event
from cara.events.contracts import Listener
from cara.events.jobs.HandleListenerJob import HandleListenerJob, _resolve_event_class
from cara.exceptions import InvalidArgumentException
from cara.queues import instantiate_job
from cara.queues.contracts import ShouldQueue
from cara.queues.serializers import SignedJsonJobSerializer

event_module = importlib.import_module("cara.events.Event")

_KEYS = {"current": "queued-listener-signing-key-" * 3}
handled: list = []


class Orders:
    class Placed:
        name = "orders.placed"

        def __init__(self, order_id: int) -> None:
            self.order_id = order_id
            self._dispatch_id = "bookkeeping"


class Billing:
    class Placed:
        name = "billing.placed"

        def __init__(self, invoice_id: int) -> None:
            self.invoice_id = invoice_id


class Shipped:
    name = "orders.shipped"

    def __init__(self, order_id: int, carrier: str = "ups") -> None:
        self.order_id = order_id
        self.carrier = carrier

    def to_dict(self):
        return {"order_id": self.order_id, "carrier": self.carrier.upper()}


class ShipListener(Listener, ShouldQueue):
    def handle(self, event) -> None:
        handled.append(event)


class _Live:
    pass


@pytest.fixture(autouse=True)
def _configuration(monkeypatch):
    monkeypatch.setattr(Configuration, "_instance", None)
    Configuration.empty()
    handled.clear()


def _dispatcher(*events) -> Event:
    dispatcher = Event()
    for event_class in events:
        dispatcher.register_event(event_class)
    dispatcher.subscribe("orders.placed", ShipListener())
    return dispatcher


def test_event_payload_is_the_public_attributes_or_to_dict() -> None:
    assert HandleListenerJob.event_payload(Orders.Placed(5)) == {"order_id": 5}
    assert HandleListenerJob.event_payload(Shipped(5)) == {
        "order_id": 5,
        "carrier": "UPS",
    }


def test_a_live_object_in_the_event_is_refused_naming_the_field() -> None:
    event = Orders.Placed(5)
    event.customer = _Live()

    with pytest.raises(InvalidArgumentException, match=r"event_data\.customer"):
        HandleListenerJob.event_payload(event)


def test_the_hop_carries_fully_qualified_names(monkeypatch) -> None:
    queued: list = []

    class _Pending:
        def __init__(self, job) -> None:
            self.job = job

        def with_routing_key(self, _key):
            return self

        def dispatch(self):
            queued.append(self.job)

    monkeypatch.setattr(event_module, "PendingDispatch", _Pending)

    assert _dispatcher()._queue_listener(ShipListener(), Orders.Placed(5)) is True

    (job,) = queued
    assert job.listener_class == f"{__name__}.ShipListener"
    assert job.event_class == f"{__name__}.Orders.Placed"
    assert job.event_data == {"order_id": 5}


def test_a_qualified_event_name_resolves_the_right_class() -> None:
    dispatcher = _dispatcher(Orders.Placed, Billing.Placed)

    assert (
        _resolve_event_class(dispatcher, f"{__name__}.Billing.Placed") is Billing.Placed
    )
    assert _resolve_event_class(dispatcher, f"{__name__}.Orders.Placed") is Orders.Placed


def test_an_ambiguous_bare_event_name_is_refused() -> None:
    dispatcher = _dispatcher(Orders.Placed, Billing.Placed)

    with pytest.raises(InvalidArgumentException, match="ambiguous"):
        _resolve_event_class(dispatcher, "Placed")


def test_an_unambiguous_bare_event_name_still_resolves() -> None:
    assert _resolve_event_class(_dispatcher(Orders.Placed), "Placed") is Orders.Placed


def _round_trip(job: HandleListenerJob) -> HandleListenerJob:
    body = SignedJsonJobSerializer.serialize(
        {
            "obj": job,
            "args": (),
            "callback": "handle",
            "created": "2026-11-23T00:00:00Z",
            "job_id": "11111111-1111-4111-8111-111111111111",
            "db_job_id": 1,
            "timeout_seconds": 300,
            "_tenant": 7,
            "_tenant_mode": "tenant",
            "queue": job.queue,
            "priority": "default",
            "dispatched_at": "2026-11-23T00:00:00Z",
        },
        signing_key_id="current",
        signing_keys=_KEYS,
        issued_at=1_763_856_000,
    )
    decoded = SignedJsonJobSerializer.deserialize(
        body, signing_keys=_KEYS, now=1_763_856_001
    )
    return instantiate_job(object(), decoded["obj"], (), decoded["init_kwargs"])


@pytest.mark.asyncio
async def test_the_hop_survives_the_signed_envelope_and_runs_the_listener(
    monkeypatch,
) -> None:
    dispatcher = _dispatcher(Orders.Placed, Billing.Placed)

    def make(key):
        if key == "events":
            return dispatcher
        raise LookupError(key)

    monkeypatch.setattr(Event, "_app", type("App", (), {"make": staticmethod(make)})())
    job = HandleListenerJob(
        listener_class=HandleListenerJob.listener_path(ShipListener),
        event_data=HandleListenerJob.event_payload(Orders.Placed(5)),
        event_class=HandleListenerJob.listener_path(Orders.Placed),
    )
    assert job.queue == "events", "default_queue reaches the attribute the driver reads"

    await _round_trip(job).handle()

    (event,) = handled
    assert type(event) is Orders.Placed
    assert event.order_id == 5
