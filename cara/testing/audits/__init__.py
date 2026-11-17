"""Test-time audits over a product's deployment, configuration and source.

An audit is the parameterized sibling of the Guard Pack scanners: the RULE
lives in the framework because the framework (or the language) is what makes it
true, and the product supplies only its own vocabulary — the coordinates of its
deployment, the names of its columns, the layers it wants scanned. Products
call an audit directly from a test, the way they already call
``audit_migrations``.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "AsyncDispatchAudit": (".AsyncDispatchAudit", "AsyncDispatchAudit"),
    "CacheKeyAudit": (".CacheKeyAudit", "CacheKeyAudit"),
    "CacheKeyFinding": (".CacheKeyFinding", "CacheKeyFinding"),
    "DISPATCH_CALLS": (".AsyncDispatchAudit", "DISPATCH_CALLS"),
    "DeployTopologyAudit": (".DeployTopologyAudit", "DeployTopologyAudit"),
    "DeployTopologyManifest": (".DeployTopologyManifest", "DeployTopologyManifest"),
    "DispatchFinding": (".DispatchFinding", "DispatchFinding"),
    "HTTPX_CLIENTS": (".HttpTimeoutAudit", "HTTPX_CLIENTS"),
    "HttpTimeoutAudit": (".HttpTimeoutAudit", "HttpTimeoutAudit"),
    "IMPLICIT_PARAMETERS": (".CacheKeyAudit", "IMPLICIT_PARAMETERS"),
    "KEYED_CACHE_METHODS": (".SaltedHashKeyAudit", "KEYED_CACHE_METHODS"),
    "MIGRATION_BASE": (".MigrationShapeAudit", "MIGRATION_BASE"),
    "MigrationShapeAudit": (".MigrationShapeAudit", "MigrationShapeAudit"),
    "MigrationShapeFinding": (".MigrationShapeFinding", "MigrationShapeFinding"),
    "MixinOverrideAudit": (".MixinOverrideAudit", "MixinOverrideAudit"),
    "MixinOverrideFinding": (".MixinOverrideFinding", "MixinOverrideFinding"),
    "ModelIndexAudit": (".ModelIndexAudit", "ModelIndexAudit"),
    "ModelIndexFinding": (".ModelIndexFinding", "ModelIndexFinding"),
    "NO_EXPORTER": (".DeployTopologyAudit", "NO_EXPORTER"),
    "NestedExceptShadowAudit": (".NestedExceptShadowAudit", "NestedExceptShadowAudit"),
    "NumericTruthinessAudit": (".NumericTruthinessAudit", "NumericTruthinessAudit"),
    "OUTBOX_ALERTS": (".DeployTopologyAudit", "OUTBOX_ALERTS"),
    "PROBE_STALE_ALERT": (".DeployTopologyAudit", "PROBE_STALE_ALERT"),
    "PROCESS_TYPE_ROLES": (".DeployTopologyAudit", "PROCESS_TYPE_ROLES"),
    "READINESS_ALERTS": (".DeployTopologyAudit", "READINESS_ALERTS"),
    "REQUEST_METHODS": (".HttpTimeoutAudit", "REQUEST_METHODS"),
    "REQUIRED_METHODS": (".MigrationShapeAudit", "REQUIRED_METHODS"),
    "REQUIRED_ROLES": (".DeployTopologyAudit", "REQUIRED_ROLES"),
    "RuntimeSafetyFinding": (".RuntimeSafetyFinding", "RuntimeSafetyFinding"),
    "SAFE_DEFAULTS": (".NumericTruthinessAudit", "SAFE_DEFAULTS"),
    "SaltedHashKeyAudit": (".SaltedHashKeyAudit", "SaltedHashKeyAudit"),
    "SaltedHashKeyFinding": (".SaltedHashKeyFinding", "SaltedHashKeyFinding"),
    "TruthinessFinding": (".TruthinessFinding", "TruthinessFinding"),
    "call_kind": (".HttpTimeoutAudit", "call_kind"),
    "scheduled_job_ids": (".DeployTopologyAudit", "scheduled_job_ids"),
}

__all__ = [
    "AsyncDispatchAudit",
    "CacheKeyAudit",
    "CacheKeyFinding",
    "DISPATCH_CALLS",
    "DeployTopologyAudit",
    "DeployTopologyManifest",
    "DispatchFinding",
    "HTTPX_CLIENTS",
    "HttpTimeoutAudit",
    "IMPLICIT_PARAMETERS",
    "KEYED_CACHE_METHODS",
    "MIGRATION_BASE",
    "MigrationShapeAudit",
    "MigrationShapeFinding",
    "MixinOverrideAudit",
    "MixinOverrideFinding",
    "ModelIndexAudit",
    "ModelIndexFinding",
    "NO_EXPORTER",
    "NestedExceptShadowAudit",
    "NumericTruthinessAudit",
    "OUTBOX_ALERTS",
    "PROBE_STALE_ALERT",
    "PROCESS_TYPE_ROLES",
    "READINESS_ALERTS",
    "REQUEST_METHODS",
    "REQUIRED_METHODS",
    "REQUIRED_ROLES",
    "RuntimeSafetyFinding",
    "SAFE_DEFAULTS",
    "SaltedHashKeyAudit",
    "SaltedHashKeyFinding",
    "TruthinessFinding",
    "call_kind",
    "scheduled_job_ids",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
