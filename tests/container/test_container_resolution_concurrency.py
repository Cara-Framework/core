"""``Container.make`` holds no lock across a constructor.

Live defect: the whole resolution ran inside the container's one lock, the
constructor call included, so a slow or blocking constructor (a client that
connects in ``__init__``, a factory that reads a file) stalled every
``make`` of every other thread, for bindings that had nothing to do with it.
The lock now covers only the registry. What it used to guarantee is kept
without it:

* a lazy singleton is still built exactly once — its factory carries a lock
  of its own, re-entrant, so a singleton that resolves itself is told so
  instead of deadlocking on that lock;
* a deferred provider whose ``register()`` raises is put back, so the next
  ``make`` runs it again instead of reporting a missing binding for the rest
  of the process;
* a second thread asking for a key whose provider is mid-registration waits
  for THAT registration and gets the binding (the race of 2026-04-24).

Real threads; every wait is on an ``Event`` with a generous timeout, and the
threads are daemons so a regression fails the test instead of hanging it.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import pytest

from cara.container import Container
from cara.exceptions import GenericContainerException

WAIT = 10.0


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


@pytest.mark.parametrize("key", ["fast", _Fast])
def test_a_slow_constructor_does_not_block_an_unrelated_make(key):
    entered, release = threading.Event(), threading.Event()

    class Slow:
        def __init__(self):
            entered.set()
            release.wait(WAIT)

    container = Container()
    container.bind("slow", Slow)
    container.bind("fast", _Fast)

    slow, slow_box = _in_thread(lambda: container.make("slow"))
    assert entered.wait(WAIT)
    try:
        fast, fast_box = _in_thread(lambda: container.make(key))
        fast.join(WAIT)
        assert not fast.is_alive(), "an unrelated make waited for a slow constructor"
        assert isinstance(fast_box["value"], _Fast)
    finally:
        release.set()
    slow.join(WAIT)
    assert isinstance(slow_box["value"], Slow)


def test_a_singleton_is_built_once_under_concurrent_first_use():
    entered, release = threading.Event(), threading.Event()
    built: list[object] = []

    def factory():
        built.append(object())
        entered.set()
        release.wait(WAIT)
        return built[-1]

    container = Container()
    container.singleton("pool", factory)

    first = _in_thread(lambda: container.make("pool"))
    assert entered.wait(WAIT)
    started = [threading.Event() for _ in range(6)]

    def late(flag: threading.Event):
        flag.set()
        return container.make("pool")

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
    container = Container()
    container.singleton("loop", lambda: container.make("loop"))

    thread, box = _in_thread(lambda: container.make("loop"))
    thread.join(WAIT)

    assert not thread.is_alive(), "a self-resolving singleton deadlocked"
    assert isinstance(box["error"], GenericContainerException)
    assert "resolves itself" in str(box["error"])


def test_a_singleton_whose_factory_raised_is_built_on_the_next_make():
    attempts: list[int] = []

    def factory():
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("cold start")
        return "ready"

    container = Container()
    container.singleton("svc", factory)

    with pytest.raises(RuntimeError, match="cold start"):
        container.make("svc")
    assert container.make("svc") == "ready"
    assert container.make("svc") == "ready"
    assert len(attempts) == 2


def test_a_provider_whose_register_raised_is_loaded_again_on_the_next_make():
    attempts: list[int] = []

    class Flaky:
        def __init__(self, application):
            self.application = application

        def register(self):
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("config not ready")
            self.application.bind("cache", "the cache")
            self.application.bind("cache.store", "the store")

    container = Container()
    container._deferred.update({"cache": Flaky, "cache.store": Flaky})

    with pytest.raises(RuntimeError, match="config not ready"):
        container.make("cache")

    assert container.has("cache") and container.has("cache.store")
    assert container.make("cache.store") == "the store"
    assert container.make("cache") == "the cache"
    assert len(attempts) == 2


def test_a_second_thread_waits_for_a_provider_mid_registration():
    entered, release = threading.Event(), threading.Event()
    registrations: list[int] = []

    class SlowProvider:
        def __init__(self, application):
            self.application = application

        def register(self):
            registrations.append(1)
            entered.set()
            release.wait(WAIT)
            self.application.bind("cache", "the cache")

    container = Container()
    container._deferred["cache"] = SlowProvider

    first, first_box = _in_thread(lambda: container.make("cache"))
    assert entered.wait(WAIT)
    try:
        second, second_box = _in_thread(lambda: container.make("cache"))
        assert _eventually(lambda: bool(container._waiting)), (
            "the second make did not wait for the registration"
        )
        assert container.has("cache"), "a key mid-registration reads as missing"
    finally:
        release.set()

    for thread in (first, second):
        thread.join(WAIT)
        assert not thread.is_alive()
    assert first_box["value"] == "the cache"
    assert second_box["value"] == "the cache"
    assert len(registrations) == 1
