"""Eloquent — layer barrel (generated, DOCTRINE §5.1). — connections subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BaseConnection": (".BaseConnection", "BaseConnection"),
    "CONNECTION_POOL": (".PostgresConnection", "CONNECTION_POOL"),
    "ConnectionFactory": (".ConnectionFactory", "ConnectionFactory"),
    "ConnectionResolver": (".ConnectionResolver", "ConnectionResolver"),
    "PostgresConnection": (".PostgresConnection", "PostgresConnection"),
    "SQLiteConnection": (".SQLiteConnection", "SQLiteConnection"),
    "regexp": (".SQLiteConnection", "regexp"),
    "reset_registry": (".ConnectionResolver", "reset_registry"),
}

__all__ = [
    "BaseConnection",
    "CONNECTION_POOL",
    "ConnectionFactory",
    "ConnectionResolver",
    "PostgresConnection",
    "SQLiteConnection",
    "regexp",
    "reset_registry",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
