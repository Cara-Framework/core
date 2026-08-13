"""Eloquent — layer barrel (generated, DOCTRINE §5.1). — query subpackage. — processors subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "PostgresPostProcessor": (".PostgresPostProcessor", "PostgresPostProcessor"),
    "SQLitePostProcessor": (".SQLitePostProcessor", "SQLitePostProcessor"),
}

__all__ = [
    "PostgresPostProcessor",
    "SQLitePostProcessor",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
