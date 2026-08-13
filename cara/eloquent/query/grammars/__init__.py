"""Eloquent — layer barrel (generated, DOCTRINE §5.1). — query subpackage. — grammars subpackage."""

from cara._LazyExports import _install_lazy_exports

from .BaseGrammar import _MULTI_SPACE_RE

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BaseGrammar": (".BaseGrammar", "BaseGrammar"),
    "PostgresGrammar": (".PostgresGrammar", "PostgresGrammar"),
    "SQLiteGrammar": (".SQLiteGrammar", "SQLiteGrammar"),
}

__all__ = [
    "BaseGrammar",
    "PostgresGrammar",
    "SQLiteGrammar",
    "_MULTI_SPACE_RE",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
