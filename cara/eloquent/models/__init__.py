"""Eloquent — layer barrel (generated, DOCTRINE §5.1). — models subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "MigrationModel": (".MigrationModel", "MigrationModel"),
    "Model": (".Model", "Model"),
    "ModelMeta": (".ModelMeta", "ModelMeta"),
    "Pivot": (".Pivot", "Pivot"),
    "ReadRow": (".ReadRow", "ReadRow"),
    "read_row_type": (".ReadRow", "read_row_type"),
    "read_rows": (".ReadRow", "read_rows"),
}

__all__ = [
    "MigrationModel",
    "Model",
    "ModelMeta",
    "Pivot",
    "ReadRow",
    "read_row_type",
    "read_rows",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
