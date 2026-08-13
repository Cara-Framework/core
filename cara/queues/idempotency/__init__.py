"""Flow-level job idempotency primitives — generic mixin."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "MakesIdempotentBase": (".MakesIdempotentBase", "MakesIdempotentBase"),
}

__all__ = [
    "MakesIdempotentBase",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
