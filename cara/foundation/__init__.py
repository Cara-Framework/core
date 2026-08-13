"""Foundation — layer barrel (generated, DOCTRINE §5.1)."""

from __future__ import annotations

import builtins
from typing import Any, TypeVar

from cara._LazyExports import _install_lazy_exports


def resolve(abstract: Any, *args: Any) -> Any:
    """Resolve a binding from the application container.

    Equivalent to Laravel's global ``resolve()`` / ``app()`` helper.
    Imports the bootstrapped application and delegates to ``Application.make()``.

    Args:
        abstract: Container key (string) or class/contract to resolve.
        *args: Extra arguments forwarded to ``Application.make()``.

    Returns:
        The resolved instance.
    """

    application = builtins.app()
    return application.make(abstract, *args)


T = TypeVar("T")


_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "Application": (".Application", "Application"),
    "DeferredProvider": (".DeferredProvider", "DeferredProvider"),
    "Provider": (".Provider", "Provider"),
}

__all__ = [
    "Any",
    "Application",
    "DeferredProvider",
    "Provider",
    "T",
    "TypeVar",
    "builtins",
    "resolve",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
