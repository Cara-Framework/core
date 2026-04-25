from typing import Any, TypeVar

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
