"""Flow-level job idempotency primitives — generic mixin."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ClaimsSourceCooldown": (".ClaimsSourceCooldown", "ClaimsSourceCooldown"),
    "MakesIdempotentBase": (".MakesIdempotentBase", "MakesIdempotentBase"),
}

__all__ = [
    "ClaimsSourceCooldown",
    "MakesIdempotentBase",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
