"""Product-blind service-layer exception family (``ServiceException`` tree)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ConflictException": (".ConflictException", "ConflictException"),
    "DuplicateEntity": (".DuplicateEntity", "DuplicateEntity"),
    "EntityNotFound": (".EntityNotFound", "EntityNotFound"),
    "Forbidden": (".Forbidden", "Forbidden"),
    "InvalidInput": (".InvalidInput", "InvalidInput"),
    "ServiceException": (".ServiceException", "ServiceException"),
    "Unauthorized": (".Unauthorized", "Unauthorized"),
    "ValidationFailed": (".ValidationFailed", "ValidationFailed"),
}

__all__ = [
    "ConflictException",
    "DuplicateEntity",
    "EntityNotFound",
    "Forbidden",
    "InvalidInput",
    "ServiceException",
    "Unauthorized",
    "ValidationFailed",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
