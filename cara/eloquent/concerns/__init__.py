"""Model Concerns Package.

Mixins that add specific functionality to Eloquent models,
following the Single Responsibility Principle.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "HasAttributes": (".HasAttributes", "HasAttributes"),
    "HasRelationships": (".HasRelationships", "HasRelationships"),
    "HasTimestamps": (".HasTimestamps", "HasTimestamps"),
    "MakesPrunable": (".MakesPrunable", "MakesPrunable"),
    "MakesPublicId": (".MakesPublicId", "MakesPublicId"),
}

__all__ = [
    "HasAttributes",
    "HasRelationships",
    "HasTimestamps",
    "MakesPrunable",
    "MakesPublicId",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
