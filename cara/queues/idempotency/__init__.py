"""Flow-level job idempotency primitives — generic mixin."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ClaimsSourceCooldown": (".ClaimsSourceCooldown", "ClaimsSourceCooldown"),
    "DerivesIdempotencyKey": (".DerivesIdempotencyKey", "DerivesIdempotencyKey"),
    "MakesIdempotentBase": (".MakesIdempotentBase", "MakesIdempotentBase"),
    "normalize_idempotency_param": (
        ".IdempotencyParams",
        "normalize_idempotency_param",
    ),
}

__all__ = [
    "ClaimsSourceCooldown",
    "DerivesIdempotencyKey",
    "MakesIdempotentBase",
    "normalize_idempotency_param",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
