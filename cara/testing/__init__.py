"""Cara testing toolkit — Pest/Laravel-style API for Python tests.

Public surface
--------------

- :func:`expect`, :class:`Expectation` — fluent assertions.
- :class:`TestCase` — base class with facade fakes + mocking helpers.
- :func:`it`, :func:`describe` — pest-style decorators.
- Facade fakes: :class:`LogFake`, :class:`MailFake`, :class:`QueueFake`,
  :class:`EventFake`, :class:`CacheFake`, :class:`NotificationFake`.
- Mocking: :class:`Mock` (strict), :class:`Spy` (permissive),
  :func:`when`, :func:`returning`.
- ``swap``, ``register``, ``reset`` — facade swap primitives if you
  need them outside ``TestCase``.

Quick start
-----------

    from cara.testing import TestCase, it, describe, expect

    class PriceValidationServiceTest(TestCase):
        auto_fakes = ("log",)

        def test_rejects_null_price(self):
            data = self.mock(PriceValidationDataContract)
            data.expects("get_latest_price_min").returns(None)
            ok, reason = PriceValidationService(data).validate(1, None)
            expect(ok).to_be_false()
            expect(reason).to_equal("Price is null")
"""

from .Expectation import Expectation, ExpectationFailed, expect
from .TestCase import TestCase
from .pest import describe, it
from .fakes import (
    CacheFake,
    EventFake,
    LogFake,
    MailFake,
    NotificationFake,
    QueueFake,
)
from .mocks import Mock, Spy, returning, when
from .facade_swap import register, reset, swap, uninstall_patch
from .loader import (
    load_contract,
    load_module,
    load_service,
    stub_modules,
    stub_modules_scoped,
)

__all__ = [
    "CacheFake",
    "EventFake",
    "Expectation",
    "ExpectationFailed",
    "LogFake",
    "MailFake",
    "Mock",
    "NotificationFake",
    "QueueFake",
    "Spy",
    "TestCase",
    "describe",
    "expect",
    "it",
    "load_contract",
    "load_module",
    "load_service",
    "register",
    "reset",
    "returning",
    "stub_modules",
    "stub_modules_scoped",
    "swap",
    "uninstall_patch",
    "when",
]
