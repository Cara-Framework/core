"""Eloquent — layer barrel (generated, DOCTRINE §5.1). — pagination subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BasePaginator": (".BasePaginator", "BasePaginator"),
    "CursorPaginator": (".CursorPaginator", "CursorPaginator"),
    "LengthAwarePaginator": (".LengthAwarePaginator", "LengthAwarePaginator"),
    "SimplePaginator": (".SimplePaginator", "SimplePaginator"),
    "keyset_operator": (".KeysetPredicate", "keyset_operator"),
    "keyset_predicate": (".KeysetPredicate", "keyset_predicate"),
}

__all__ = [
    "BasePaginator",
    "CursorPaginator",
    "LengthAwarePaginator",
    "SimplePaginator",
    "keyset_operator",
    "keyset_predicate",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
