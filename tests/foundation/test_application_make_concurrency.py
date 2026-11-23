"""``Application.make`` holds no lock across a provider or a constructor.

Live defect: ``Application.make`` closed the deferred-provider race of
2026-04-24 by holding one lock across the provider's ``register()``/
``boot()`` AND ``super().make()``, so every resolution in the process was
serialised behind whichever constructor was running. It also popped the
provider's keys before ``register()``: a ``register()`` that raised left
them in neither table, and ``has(key)`` read False for a key that was
being registered that very moment. Deferred providers now load through
``Container.load_deferred`` (a waiting thread waits for THAT provider only;
a failed ``register()`` puts the keys back), and ``has`` reports a key
mid-registration as present.

The application is built without ``__init__``'s environment loading, which
these properties do not touch. Real threads; every wait is on an ``Event``
with a generous timeout, and the threads are daemons so a regression fails
the test instead of hanging it.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import pytest

from cara.container import Container
from cara.exceptions import GenericContainerException
from cara.foundation.Application import Application
from cara.foundation.DeferredProvider import DeferredProvider

WAIT = 10.0


def _application() -> Application:
    app = Application.__new__(Application)
    Container.__init__(app)
    app.base_path = None
    app.storage_path = None
    app.providers = []
    app.deferred_providers = {}
    return app


def _in_thread(work: Callable[[], Any]) -> tuple[threading.Thread, dict[str, Any]]:
    """Run ``work`` on a daemon thread; its result or error lands in the box."""
    box: dict[str, Any] = {}

    def run() -> None:
        try:
            box["value"] = work()
        except BaseException as error:  # noqa: BLE001 — reported by the test
            box["error"] = error

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, box


def _eventually(condition: Callable[[], bool]) -> bool:
    """Whether ``condition`` holds within ``WAIT`` seconds (bounded poll)."""
    tick = threading.Event()
    for _ in range(int(WAIT * 1000)):
        if condition():
            return True
        tick.wait(0.001)
    return condition()


class _Fast:
    pass


def test_a_slow_constructor_does_not_block_an_unrelated_make():
    entered, release = threading.Event(), threading.Event()

    class Slow:
        def __init__(self):
            entered.set()
            release.wait(WAIT)

    app = _application()
    app.bind("slow", Slow)
    app.bind("fast", _Fast)

    slow, slow_box = _in_thread(lambda: app.make("slow"))
    assert entered.wait(WAIT)
    try:
        fast, fast_box = _in_thread(lambda: app.make("fast"))
        fast.join(WAIT)
        assert not fast.is_alive(), "an unrelated make waited for a slow constructor"
        assert isinstance(fast_box["value"], _Fast)
    finally:
        release.set()
    slow.join(WAIT)
    assert isinstance(slow_box["value"], Slow)


def test_a_slow_deferred_provider_does_not_block_an_unrelated_make():
    entered, release = threading.Event(), threading.Event()

    class MailProvider(DeferredProvider):
        @classmethod
        def provides(cls):
            return ["mail"]

        def register(self):
            entered.set()
            release.wait(WAIT)
            self.application.bind("mail", "the mailer")

    app = _application()
    app.add_providers(MailProvider)
    app.bind("fast", _Fast)

    slow, slow_box = _in_thread(lambda: app.make("mail"))
    assert entered.wait(WAIT)
    try:
        fast, fast_box = _in_thread(lambda: app.make("fast"))
        fast.join(WAIT)
        assert not fast.is_alive(), "a make waited for another key's provider"
        assert isinstance(fast_box["value"], _Fast)
    finally:
        release.set()
    slow.join(WAIT)
    assert slow_box["value"] == "the mailer"


def test_a_singleton_is_built_once_under_concurrent_first_use():
    entered, release = threading.Event(), threading.Event()
    built: list[object] = []

    def factory():
        built.append(object())
        entered.set()
        release.wait(WAIT)
        return built[-1]

    app = _application()
    app.singleton("pool", factory)

    first = _in_thread(lambda: app.make("pool"))
    assert entered.wait(WAIT)
    started = [threading.Event() for _ in range(6)]

    def late(flag: threading.Event):
        flag.set()
        return app.make("pool")

    others = [_in_thread(lambda flag=flag: late(flag)) for flag in started]
    assert all(flag.wait(WAIT) for flag in started)
    release.set()

    results = []
    for thread, box in [first, *others]:
        thread.join(WAIT)
        assert not thread.is_alive()
        results.append(box["value"])
    assert len(built) == 1, f"the singleton factory ran {len(built)} times"
    assert all(result is built[0] for result in results)


def test_a_singleton_that_resolves_itself_raises_instead_of_hanging():
    app = _application()
    app.singleton("loop", lambda: app.make("loop"))

    thread, box = _in_thread(lambda: app.make("loop"))
    thread.join(WAIT)

    assert not thread.is_alive(), "a self-resolving singleton deadlocked"
    assert isinstance(box["error"], GenericContainerException)


def test_a_provider_whose_register_raised_is_loaded_again_on_the_next_make():
    attempts: list[int] = []

    class CacheProvider(DeferredProvider):
        @classmethod
        def provides(cls):
            return ["cache", "cache.store"]

        def register(self):
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("config not ready")
            self.application.bind("cache", "the cache")
            self.application.bind("cache.store", "the store")

    app = _application()
    app.add_providers(CacheProvider)

    with pytest.raises(RuntimeError, match="config not ready"):
        app.make("cache")

    assert app.has("cache") and app.has("cache.store")
    assert app.make("cache.store") == "the store"
    assert app.make("cache") == "the cache"
    assert len(attempts) == 2
    assert app.deferred_providers == {}


def test_a_class_key_loads_its_deferred_provider():
    class Mailer:
        pass

    class MailerProvider(DeferredProvider):
        @classmethod
        def provides(cls):
            return ["mailer"]

        def register(self):
            self.application.bind(Mailer, Mailer)

    app = _application()
    app.add_providers(MailerProvider)

    assert app.has(Mailer)
    assert isinstance(app.make(Mailer), Mailer)
    assert app.deferred_providers == {}


def test_a_key_mid_registration_is_present_and_a_second_make_waits_for_it():
    entered, release = threading.Event(), threading.Event()
    registrations: list[int] = []

    class CacheProvider(DeferredProvider):
        @classmethod
        def provides(cls):
            return ["cache"]

        def register(self):
            registrations.append(1)
            entered.set()
            release.wait(WAIT)
            self.application.bind("cache", "the cache")

    app = _application()
    app.add_providers(CacheProvider)

    first, first_box = _in_thread(lambda: app.make("cache"))
    assert entered.wait(WAIT)
    try:
        assert "cache" not in app.deferred_providers  # left the deferred table
        assert app.has("cache"), "a key mid-registration reads as missing"
        second, second_box = _in_thread(lambda: app.make("cache"))
        assert _eventually(lambda: bool(app._waiting)), (
            "the second make did not wait for the registration"
        )
    finally:
        release.set()

    for thread in (first, second):
        thread.join(WAIT)
        assert not thread.is_alive()
    assert first_box["value"] == "the cache"
    assert second_box["value"] == "the cache"
    assert len(registrations) == 1
