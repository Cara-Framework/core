"""Base test class for the Cara framework.

Subclass this to get:
- ``self.fake_log()`` / ``fake_mail()`` / ``fake_queue()`` / etc — install
  facade fakes that auto-restore on tear-down.
- ``self.mock(Contract)`` / ``self.spy(Contract)`` — fluent contract mocks.
- ``self.expect(value)`` — bound version of :func:`expect` so tests
  read like ``self.expect(x).to_be(...)``.

Each test gets a clean fake registry; nothing leaks between tests.
"""

from __future__ import annotations

import unittest
from typing import Any, Optional, Type

from .Expectation import expect as _expect
from .Expectation import Expectation
from .facade_swap import register, reset, uninstall_patch
from .fakes import (
    CacheFake,
    EventFake,
    LogFake,
    MailFake,
    NotificationFake,
    QueueFake,
)
from .mocks import Mock, Spy


class TestCase(unittest.TestCase):
    """Base for all Cara test cases.

    Inherits from ``unittest.TestCase`` so it works out of the box with
    pytest, unittest, and any IDE that runs either. The Pest-style
    ``expect()`` is preferred over ``assertEqual``; both work.
    """

    # Subclasses can override these to auto-install fakes on every test.
    auto_fakes: tuple = ()

    # ── Lifecycle ────────────────────────────────────────────────────

    def setUp(self) -> None:
        super().setUp()
        # Per-test fake registry — assigned only on demand.
        self._fakes: dict = {}
        for name in self.auto_fakes:
            getattr(self, f"fake_{name}")()

    def tearDown(self) -> None:
        # Drop all fakes back to no-op state so nothing carries over.
        reset()
        self._fakes.clear()
        super().tearDown()

    @classmethod
    def tearDownClass(cls) -> None:
        # Conservatively un-patch the metaclass when an entire class
        # has finished — keeps non-cara tests pristine.
        uninstall_patch()
        super().tearDownClass()

    # ── Fake helpers ─────────────────────────────────────────────────

    def fake_log(self) -> LogFake:
        fake = LogFake()
        register("logger", fake)  # Cara's Log facade key
        self._fakes["log"] = fake
        return fake

    def fake_mail(self) -> MailFake:
        fake = MailFake()
        register("mail", fake)
        self._fakes["mail"] = fake
        return fake

    def fake_queue(self) -> QueueFake:
        fake = QueueFake()
        register("queue", fake)
        self._fakes["queue"] = fake
        return fake

    def fake_event(self) -> EventFake:
        fake = EventFake()
        register("event", fake)
        self._fakes["event"] = fake
        return fake

    def fake_cache(self) -> CacheFake:
        fake = CacheFake()
        register("cache", fake)
        self._fakes["cache"] = fake
        return fake

    def fake_notification(self) -> NotificationFake:
        fake = NotificationFake()
        register("notification", fake)
        self._fakes["notification"] = fake
        return fake

    # Lookup helpers — useful when ``auto_fakes = ("log",)``.
    def get_fake(self, name: str) -> Any:
        if name not in self._fakes:
            raise KeyError(f"No fake named {name!r}; call fake_{name}() first.")
        return self._fakes[name]

    @property
    def log(self) -> LogFake:
        return self.get_fake("log")

    @property
    def mail(self) -> MailFake:
        return self.get_fake("mail")

    @property
    def queue(self) -> QueueFake:
        return self.get_fake("queue")

    @property
    def cache(self) -> CacheFake:
        return self.get_fake("cache")

    # ── Mocking ──────────────────────────────────────────────────────

    def mock(self, contract: Optional[Type[Any]] = None) -> Mock:
        return Mock(contract)

    def spy(self, contract: Optional[Type[Any]] = None) -> Spy:
        return Spy(contract)

    # ── Expectation sugar ────────────────────────────────────────────

    def expect(self, value: Any, *, label: Optional[str] = None) -> Expectation:
        return _expect(value, label=label)
