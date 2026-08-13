"""Cara's production query builder."""

from cara._LazyExports import _install_lazy_exports

from .grammars import _MULTI_SPACE_RE

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BaseGrammar": (".grammars", "BaseGrammar"),
    "EagerRelations": (".EagerRelations", "EagerRelations"),
    "ORDER_BY_COLUMN_RE": ("._QuerySafety", "ORDER_BY_COLUMN_RE"),
    "PostgresGrammar": (".grammars", "PostgresGrammar"),
    "PostgresPostProcessor": (".processors", "PostgresPostProcessor"),
    "QueryBuilder": (".QueryBuilder", "QueryBuilder"),
    "SQLiteGrammar": (".grammars", "SQLiteGrammar"),
    "SQLitePostProcessor": (".processors", "SQLitePostProcessor"),
    "TransactionContext": (".TransactionContext", "TransactionContext"),
}

__all__ = [
    "BaseGrammar",
    "EagerRelations",
    "ORDER_BY_COLUMN_RE",
    "PostgresGrammar",
    "PostgresPostProcessor",
    "QueryBuilder",
    "SQLiteGrammar",
    "SQLitePostProcessor",
    "TransactionContext",
    "_MULTI_SPACE_RE",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
