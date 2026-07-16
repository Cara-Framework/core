"""Security primitives."""

from .SigningKeys import require_independent_signing_key, require_signing_keyring

__all__ = ["require_independent_signing_key", "require_signing_keyring"]
