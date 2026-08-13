"""Eloquent — layer barrel (generated, DOCTRINE §5.1). — scopes subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BaseScope": (".BaseScope", "BaseScope"),
    "MakesSoftDeletes": (".MakesSoftDeletes", "MakesSoftDeletes"),
    "MakesTenantScope": (".MakesTenantScope", "MakesTenantScope"),
    "MakesTimestamps": (".MakesTimestamps", "MakesTimestamps"),
    "SoftDeleteScope": (".SoftDeleteScope", "SoftDeleteScope"),
    "TenantScope": (".TenantScope", "TenantScope"),
    "TimeStampsScope": (".TimeStampsScope", "TimeStampsScope"),
}

__all__ = [
    "BaseScope",
    "MakesSoftDeletes",
    "MakesTenantScope",
    "MakesTimestamps",
    "SoftDeleteScope",
    "TenantScope",
    "TimeStampsScope",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
