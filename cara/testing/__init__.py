"""Cara testing toolkit — Pest/Laravel-style API for Python tests.

Public surface
--------------

- :func:`expect`, :class:`Expectation` — fluent assertions.
- :class:`TestCase` — base class with facade fakes + mocking helpers.
- :func:`it`, :func:`describe` — pest-style decorators.
- Facade fakes: :class:`LogFake`, :class:`MailFake`, :class:`QueueFake`,
  :class:`EventFake`, :class:`CacheFake`, :class:`NotificationFake`.
- Mocking: :class:`Mock` (strict), :class:`Spy` (permissive).
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

from cara._LazyExports import _install_lazy_exports

from . import FacadeSwap as facade_swap

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "AsyncDispatchAudit": (".audits", "AsyncDispatchAudit"),
    "CacheFake": (".fakes", "CacheFake"),
    "CacheKeyAudit": (".audits", "CacheKeyAudit"),
    "CacheKeyFinding": (".audits", "CacheKeyFinding"),
    "DISPATCH_CALLS": (".audits", "DISPATCH_CALLS"),
    "DatabaseTransactions": (".DatabaseTransactions", "DatabaseTransactions"),
    "DeployTopologyAudit": (".audits", "DeployTopologyAudit"),
    "DeployTopologyManifest": (".audits", "DeployTopologyManifest"),
    "DispatchFinding": (".audits", "DispatchFinding"),
    "DispatchedEvent": (".fakes", "DispatchedEvent"),
    "EventFake": (".fakes", "EventFake"),
    "Expectation": (".Expectation", "Expectation"),
    "ExpectationFailed": (".ExpectationFailed", "ExpectationFailed"),
    "F": (".Pest", "F"),
    "IMPLICIT_PARAMETERS": (".audits", "IMPLICIT_PARAMETERS"),
    "LogFake": (".fakes", "LogFake"),
    "LogRecord": (".fakes", "LogRecord"),
    "MailFake": (".fakes", "MailFake"),
    "Mock": (".mocks", "Mock"),
    "NO_EXPORTER": (".audits", "NO_EXPORTER"),
    "NotificationFake": (".fakes", "NotificationFake"),
    "NumericTruthinessAudit": (".audits", "NumericTruthinessAudit"),
    "OUTBOX_ALERTS": (".audits", "OUTBOX_ALERTS"),
    "PROBE_STALE_ALERT": (".audits", "PROBE_STALE_ALERT"),
    "PROCESS_TYPE_ROLES": (".audits", "PROCESS_TYPE_ROLES"),
    "QueueFake": (".fakes", "QueueFake"),
    "QueuedJob": (".fakes", "QueuedJob"),
    "READINESS_ALERTS": (".audits", "READINESS_ALERTS"),
    "REQUIRED_ROLES": (".audits", "REQUIRED_ROLES"),
    "SAFE_DEFAULTS": (".audits", "SAFE_DEFAULTS"),
    "SentMail": (".fakes", "SentMail"),
    "SentNotification": (".fakes", "SentNotification"),
    "Spy": (".mocks", "Spy"),
    "TestCase": (".TestCase", "TestCase"),
    "TruthinessFinding": (".audits", "TruthinessFinding"),
    "describe": (".Pest", "describe"),
    "expect": (".Expectation", "expect"),
    "install_patch": (".FacadeSwap", "install_patch"),
    "it": (".Pest", "it"),
    "load_contract": (".Loader", "load_contract"),
    "load_module": (".Loader", "load_module"),
    "load_service": (".Loader", "load_service"),
    "register": (".FacadeSwap", "register"),
    "reset": (".FacadeSwap", "reset"),
    "scheduled_job_ids": (".audits", "scheduled_job_ids"),
    "stub_modules": (".Loader", "stub_modules"),
    "stub_modules_scoped": (".Loader", "stub_modules_scoped"),
    "swap": (".FacadeSwap", "swap"),
    "uninstall_patch": (".FacadeSwap", "uninstall_patch"),
    "unregister": (".FacadeSwap", "unregister"),
}

__all__ = [
    "AsyncDispatchAudit",
    "CacheFake",
    "CacheKeyAudit",
    "CacheKeyFinding",
    "DISPATCH_CALLS",
    "DatabaseTransactions",
    "DeployTopologyAudit",
    "DeployTopologyManifest",
    "DispatchFinding",
    "DispatchedEvent",
    "EventFake",
    "Expectation",
    "ExpectationFailed",
    "F",
    "IMPLICIT_PARAMETERS",
    "LogFake",
    "LogRecord",
    "MailFake",
    "Mock",
    "NO_EXPORTER",
    "NotificationFake",
    "NumericTruthinessAudit",
    "OUTBOX_ALERTS",
    "PROBE_STALE_ALERT",
    "PROCESS_TYPE_ROLES",
    "QueueFake",
    "QueuedJob",
    "READINESS_ALERTS",
    "REQUIRED_ROLES",
    "SAFE_DEFAULTS",
    "SentMail",
    "SentNotification",
    "Spy",
    "TestCase",
    "TruthinessFinding",
    "describe",
    "expect",
    "facade_swap",
    "install_patch",
    "it",
    "load_contract",
    "load_module",
    "load_service",
    "register",
    "reset",
    "scheduled_job_ids",
    "stub_modules",
    "stub_modules_scoped",
    "swap",
    "uninstall_patch",
    "unregister",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
