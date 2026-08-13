"""
HTTP Request Utilities Package.

This package contains utility classes and functions for HTTP request processing,
including query string parsing and input validation.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "KeyPart": (".KeyPart", "KeyPart"),
    "QueryStringParser": (".QueryStringParser", "QueryStringParser"),
}

__all__ = [
    "KeyPart",
    "QueryStringParser",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
