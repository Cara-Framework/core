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
    "IMPLICIT_PARAMETERS": (".CacheKeyAudit", "IMPLICIT_PARAMETERS"),
    "MIGRATION_BASE": (".MigrationShapeAudit", "MIGRATION_BASE"),
    "MigrationShapeAudit": (".MigrationShapeAudit", "MigrationShapeAudit"),
    "MigrationShapeFinding": (".MigrationShapeFinding", "MigrationShapeFinding"),
    "NO_EXPORTER": (".DeployTopologyAudit", "NO_EXPORTER"),
    "REQUIRED_METHODS": (".MigrationShapeAudit", "REQUIRED_METHODS"),
    "NumericTruthinessAudit": (".NumericTruthinessAudit", "NumericTruthinessAudit"),
    "OUTBOX_ALERTS": (".DeployTopologyAudit", "OUTBOX_ALERTS"),
    "PROBE_STALE_ALERT": (".DeployTopologyAudit", "PROBE_STALE_ALERT"),
    "PROCESS_TYPE_ROLES": (".DeployTopologyAudit", "PROCESS_TYPE_ROLES"),
    "READINESS_ALERTS": (".DeployTopologyAudit", "READINESS_ALERTS"),
    "REQUIRED_ROLES": (".DeployTopologyAudit", "REQUIRED_ROLES"),
    "SAFE_DEFAULTS": (".NumericTruthinessAudit", "SAFE_DEFAULTS"),
    "TruthinessFinding": (".TruthinessFinding", "TruthinessFinding"),
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
    "IMPLICIT_PARAMETERS",
    "MIGRATION_BASE",
    "MigrationShapeAudit",
    "MigrationShapeFinding",
    "NO_EXPORTER",
    "NumericTruthinessAudit",
    "OUTBOX_ALERTS",
    "PROBE_STALE_ALERT",
    "PROCESS_TYPE_ROLES",
    "READINESS_ALERTS",
    "REQUIRED_METHODS",
    "REQUIRED_ROLES",
    "SAFE_DEFAULTS",
    "TruthinessFinding",
    "scheduled_job_ids",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
