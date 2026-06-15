from __future__ import annotations

from typing import Any, TypeVar

# Import Provider (and DeferredProvider) BEFORE Application: Application's
# import chain pulls in providers that do `from cara.foundation import Provider`
# while this package is mid-init. Binding Provider first prevents Python from
# returning the submodule instead of the class (circular-import at boot).
from .Provider import Provider
from .DeferredProvider import DeferredProvider
from .Application import Application

T = TypeVar("T")


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
    from bootstrap import application

    return application.make(abstract, *args)


__all__ = [
    "Application",
    "DeferredProvider",
    "Provider",
    "resolve",
]
