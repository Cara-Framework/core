"""
HTTP Request Mixins Package.

This package contains mixin classes that provide specific functionality to the Request class,
promoting separation of concerns and maintainability.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "MakesBodyParsing": (".MakesBodyParsing", "MakesBodyParsing"),
    "MakesRequestHelpers": (".MakesRequestHelpers", "MakesRequestHelpers"),
    "MakesValidationHelpers": (".MakesValidationHelpers", "MakesValidationHelpers"),
}

__all__ = [
    "MakesBodyParsing",
    "MakesRequestHelpers",
    "MakesValidationHelpers",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
