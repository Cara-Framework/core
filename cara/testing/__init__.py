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
- Test support: :func:`unique` / :func:`public_id` (id minters),
  :func:`drop_modules` (leak-free module re-import),
  :func:`assert_cli_prints` / :func:`strip_sgr` (craft output under CI),
  :func:`terminal_name` / :func:`dotted_name` (AST names for source guards).

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
    "MIGRATION_BASE": (".audits", "MIGRATION_BASE"),
    "MailFake": (".fakes", "MailFake"),
    "MigrationShapeAudit": (".audits", "MigrationShapeAudit"),
    "MigrationShapeFinding": (".audits", "MigrationShapeFinding"),
    "Mock": (".mocks", "Mock"),
    "NO_EXPORTER": (".audits", "NO_EXPORTER"),
    "NotificationFake": (".fakes", "NotificationFake"),
    "NumericTruthinessAudit": (".audits", "NumericTruthinessAudit"),
    "OUTBOX_ALERTS": (".audits", "OUTBOX_ALERTS"),
    "PROBE_STALE_ALERT": (".audits", "PROBE_STALE_ALERT"),
    "PROCESS_TYPE_ROLES": (".audits", "PROCESS_TYPE_ROLES"),
    "PUBLIC_ID_LENGTH": (".Minters", "PUBLIC_ID_LENGTH"),
    "QueryCounter": (".QueryCounter", "QueryCounter"),
    "QueueFake": (".fakes", "QueueFake"),
    "QueuedJob": (".fakes", "QueuedJob"),
    "READINESS_ALERTS": (".audits", "READINESS_ALERTS"),
    "REQUIRED_METHODS": (".audits", "REQUIRED_METHODS"),
    "REQUIRED_ROLES": (".audits", "REQUIRED_ROLES"),
    "SAFE_DEFAULTS": (".audits", "SAFE_DEFAULTS"),
    "SentMail": (".fakes", "SentMail"),
    "SentNotification": (".fakes", "SentNotification"),
    "Spy": (".mocks", "Spy"),
    "TestCase": (".TestCase", "TestCase"),
    "TruthinessFinding": (".audits", "TruthinessFinding"),
    "assert_cli_prints": (".CliOutput", "assert_cli_prints"),
    "assert_query_count": (".QueryCounter", "assert_query_count"),
    "count_queries": (".QueryCounter", "count_queries"),
    "describe": (".Pest", "describe"),
    "dotted_name": (".AstNames", "dotted_name"),
    "drop_modules": (".ModuleIsolation", "drop_modules"),
    "expect": (".Expectation", "expect"),
    "install_patch": (".FacadeSwap", "install_patch"),
    "it": (".Pest", "it"),
    "load_contract": (".Loader", "load_contract"),
    "load_module": (".Loader", "load_module"),
    "load_service": (".Loader", "load_service"),
    "public_id": (".Minters", "public_id"),
    "register": (".FacadeSwap", "register"),
    "reset": (".FacadeSwap", "reset"),
    "scheduled_job_ids": (".audits", "scheduled_job_ids"),
    "strip_sgr": (".CliOutput", "strip_sgr"),
    "stub_modules": (".Loader", "stub_modules"),
    "stub_modules_scoped": (".Loader", "stub_modules_scoped"),
    "swap": (".FacadeSwap", "swap"),
    "terminal_name": (".AstNames", "terminal_name"),
    "uninstall_patch": (".FacadeSwap", "uninstall_patch"),
    "unique": (".Minters", "unique"),
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
    "MIGRATION_BASE",
    "MailFake",
    "MigrationShapeAudit",
    "MigrationShapeFinding",
    "Mock",
    "NO_EXPORTER",
    "NotificationFake",
    "NumericTruthinessAudit",
    "OUTBOX_ALERTS",
    "PROBE_STALE_ALERT",
    "PROCESS_TYPE_ROLES",
    "PUBLIC_ID_LENGTH",
    "QueryCounter",
    "QueueFake",
    "QueuedJob",
    "READINESS_ALERTS",
    "REQUIRED_METHODS",
    "REQUIRED_ROLES",
    "SAFE_DEFAULTS",
    "SentMail",
    "SentNotification",
    "Spy",
    "TestCase",
    "TruthinessFinding",
    "assert_cli_prints",
    "assert_query_count",
    "count_queries",
    "describe",
    "dotted_name",
    "drop_modules",
    "expect",
    "facade_swap",
    "install_patch",
    "it",
    "load_contract",
    "load_module",
    "load_service",
    "public_id",
    "register",
    "reset",
    "scheduled_job_ids",
    "strip_sgr",
    "stub_modules",
    "stub_modules_scoped",
    "swap",
    "terminal_name",
    "uninstall_patch",
    "unique",
    "unregister",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
