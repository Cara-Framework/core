"""Eloquent — layer barrel (generated, DOCTRINE §5.1). — migrations subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "FieldDiff": (".FieldDiff", "FieldDiff"),
    "Migration": (".Migration", "Migration"),
    "MigrationColumn": (".MigrationColumn", "MigrationColumn"),
    "MigrationExecutor": (".MigrationExecutor", "MigrationExecutor"),
    "MigrationFileManager": (".MigrationFileManager", "MigrationFileManager"),
    "MigrationGenerator": (".MigrationGenerator", "MigrationGenerator"),
    "MigrationTracker": (".MigrationTracker", "MigrationTracker"),
    "ModelDiscoverer": (".ModelDiscoverer", "ModelDiscoverer"),
    "ModelMigrationComparator": (
        ".ModelMigrationComparator",
        "ModelMigrationComparator",
    ),
    "migration_table_actions": (".ModelMigrationComparator", "migration_table_actions"),
    "summarize_change_name": (".ModelMigrationComparator", "summarize_change_name"),
}

__all__ = [
    "FieldDiff",
    "Migration",
    "MigrationColumn",
    "MigrationExecutor",
    "MigrationFileManager",
    "MigrationGenerator",
    "MigrationTracker",
    "ModelDiscoverer",
    "ModelMigrationComparator",
    "migration_table_actions",
    "summarize_change_name",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
