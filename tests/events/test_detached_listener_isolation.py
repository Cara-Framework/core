"""Listener hops that ran in the wrong place.

A detached ``fire_sync`` / ``fire_after_commit`` task shared the CALLER's
transaction registry, so a listener's write joined whatever transaction
the caller had open when the task ran and rolled back with it. And a
queued listener was looked up by its bare class name, so a hop for
``billing.NotifyListener`` ran ``orders.NotifyListener`` — whichever
subscribed first.
"""

from __future__ import annotations

import asyncio
import contextvars
from types import ModuleType

import pytest

from cara.eloquent.connections.ConnectionResolver import _get_registry
from cara.events import Event
from cara.events.jobs.HandleListenerJob import HandleListenerJob, _resolve_listener_class
from cara.exceptions import ListenerNotFoundException

_REQUEST_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_test_request_id", default=None
)


class _OrderArchived:
    name = "order.archived"


def _install(monkeypatch, dispatcher: Event) -> None:
    app = type("App", (), {"make": staticmethod(lambda _key: dispatcher)})()
    monkeypatch.setattr(Event, "_app", app)


class TestDetachedListenerTransaction:
    def test_the_detached_task_gets_its_own_registry_and_keeps_other_context(
        self, monkeypatch
    ) -> None:
        seen: list[tuple[bool, str | None]] = []

        class _Peek:
            async def handle(self, _event) -> None:
                registry = _get_registry()
                seen.append(("app" in registry, _REQUEST_ID.get()))
                registry["listener"] = object()

        dispatcher = Event()
        dispatcher.subscribe("order.archived", _Peek())
        _install(monkeypatch, dispatcher)

        async def caller() -> dict:
            _REQUEST_ID.set("req-1")
            registry = _get_registry()
            registry["app"] = object()  # the caller's open transaction
            Event.fire_sync(_OrderArchived())
            await asyncio.sleep(0.01)
            return registry

        caller_registry = asyncio.run(caller())

        assert seen == [(False, "req-1")]
        assert "listener" not in caller_registry


def _listener_class(module_name: str, ran: list[str]) -> type:
    module = ModuleType(module_name)

    class NotifyListener:
        def handle(self, _event) -> None:
            ran.append(module_name)

    NotifyListener.__module__ = module_name
    NotifyListener.__qualname__ = "NotifyListener"
    module.NotifyListener = NotifyListener
    return NotifyListener


class TestQueuedListenerName:
    @pytest.fixture
    def dispatcher(self) -> tuple[Event, type, type]:
        ran: list[str] = []
        orders = _listener_class("app.listeners.orders", ran)
        billing = _listener_class("app.listeners.billing", ran)
        dispatcher = Event()
        dispatcher.subscribe("order.shipped", orders())
        dispatcher.subscribe("invoice.paid", billing())
        return dispatcher, orders, billing

    def test_the_hop_carries_the_fully_qualified_name(self, dispatcher) -> None:
        _dispatcher, _orders, billing = dispatcher

        assert (
            HandleListenerJob.listener_path(billing)
            == "app.listeners.billing.NotifyListener"
        )

    def test_a_qualified_name_resolves_the_right_class(self, dispatcher) -> None:
        events, orders, billing = dispatcher

        assert (
            _resolve_listener_class(events, "app.listeners.billing.NotifyListener")
            is billing
        )
        assert (
            _resolve_listener_class(events, "app.listeners.orders.NotifyListener")
            is orders
        )

    def test_an_ambiguous_bare_name_is_refused(self, dispatcher) -> None:
        events, _orders, _billing = dispatcher

        with pytest.raises(ListenerNotFoundException, match="ambiguous"):
            _resolve_listener_class(events, "NotifyListener")

    def test_an_unambiguous_bare_name_still_resolves(self) -> None:
        only = _listener_class("app.listeners.solo", [])
        events = Event()
        events.subscribe("order.shipped", only())

        assert _resolve_listener_class(events, "NotifyListener") is only

    def test_an_unknown_name_is_refused(self, dispatcher) -> None:
        events, _orders, _billing = dispatcher

        with pytest.raises(ListenerNotFoundException):
            _resolve_listener_class(events, "app.listeners.nowhere.NotifyListener")
