"""Password strength policy — which plaintext is worth hashing at all.

``cara.encryption`` decides how a password is stored; this module decides
whether the application is willing to store it. It follows NIST SP 800-63B:
no composition rules ("must contain a symbol" buys nothing and costs
usability), only bounds that reject trivially-guessable and pathologically
expensive input.

THE BYTE CEILING IS TWO RULES SHARING ONE NAME
----------------------------------------------
Products kept re-deriving this ceiling and landed on different numbers
because it answers two independent questions:

1. **Cost.** A memory-hard KDF does work proportional to its input, so an
   unbounded password field on an unauthenticated route is a CPU amplifier a
   stranger can pull. That bound is a policy preference, and it is
   configurable: ``auth.password_max_bytes`` (default ``MAX_PASSWORD_BYTES``).

2. **Truncation.** bcrypt authenticates only its first 72 bytes, so every
   longer password collapses into a suffix-equivalence class — and because
   ``BcryptHasher.check`` refuses over-long input outright, a product that
   accepts a 100-byte password over bcrypt storage mints accounts that can be
   created and then never signed into. That bound is not a preference, it is
   a property of the algorithm. So it **clamps** the configured ceiling
   instead of sitting beside it, and configuration can never raise past it.

Both numbers survive here, each doing its own job. The truncation bound is
read from the algorithm passwords are STORED with —
``auth.password_hash_algorithm``, defaulting to ``Hash.DEFAULT_ALGORITHM`` —
via ``Hash.truncation_boundary``, so a product that pins bcrypt inherits 72
automatically and one on Argon2id keeps the full cost bound. Neither product
has to remember why the number is what it is.

Every threshold is read at CALL time, never at import. This module is pulled
in while middleware configuration is still loading — before the container
holds ``config`` at all — and an import-time snapshot could not see a value
the environment sets later anyway.
"""

from __future__ import annotations

from cara.configuration import config
from cara.encryption import Hash
from cara.exceptions import AuthenticationConfigurationException, InvalidArgumentException

# Cost bound (see the module docstring). Generous on purpose: the point is to
# refuse a megabyte, not to argue with a passphrase.
MAX_PASSWORD_BYTES = 1024

# Length alone does not stop "aaaaaaaaaaaaaaaa".
MIN_UNIQUE_CHARS = 4

# Prefix matching rather than exact matching: "password1", "password!23" and
# "Password2026" are the same guess wearing a hat.
COMMON_PASSWORD_PREFIXES = (
    "password",
    "qwerty",
    "123456",
    "111111",
    "abc123",
    "letmein",
    "admin",
    "welcome",
    "iloveyou",
)


def password_max_bytes() -> int:
    """The effective byte ceiling: the configured cost bound, clamped by the
    storage algorithm's truncation boundary.

    Callers that pre-bound a password before hashing (a login path, say) must
    use THIS, not ``MAX_PASSWORD_BYTES`` — the constant is only the default
    for the configurable half.
    """
    ceiling = _configured_positive_int(
        "auth.password_max_bytes",
        config("auth.password_max_bytes"),
        MAX_PASSWORD_BYTES,
    )
    algorithm = _storage_algorithm()
    try:
        boundary = Hash.truncation_boundary(algorithm)
    except InvalidArgumentException as exc:
        raise AuthenticationConfigurationException(
            f"auth.password_hash_algorithm names unsupported algorithm {algorithm!r}"
        ) from exc
    if boundary is None:
        return ceiling
    return min(ceiling, boundary)


def check_password_strength(password: str) -> str | None:
    """Return a user-facing error for a weak or oversized password, else None."""
    if not isinstance(password, str):
        raise TypeError("password must be text")
    ceiling = password_max_bytes()
    if len(password.encode("utf-8")) > ceiling:
        return (
            f"Password must be at most {ceiling} bytes (the encoded value is too large)."
        )
    minimum_unique = _min_unique_chars()
    if len(set(password)) < minimum_unique:
        return f"Password must contain at least {minimum_unique} different characters."
    lowered = password.lower()
    for prefix in denied_prefixes():
        if lowered.startswith(prefix):
            return "This password is too common. Please choose a different password."
    return None


def denied_prefixes() -> tuple[str, ...]:
    """The framework deny-list, plus anything ``auth.password_denied_prefixes``
    adds. A product extends the list; it cannot shrink it, because a shorter
    deny-list is never the fix for a rejected password."""
    extra = config("auth.password_denied_prefixes")
    if extra is None:
        return COMMON_PASSWORD_PREFIXES
    if not isinstance(extra, (list, tuple)):
        raise AuthenticationConfigurationException(
            "auth.password_denied_prefixes must be a list of non-empty strings"
        )
    normalized: list[str] = []
    for prefix in extra:
        if not isinstance(prefix, str) or not prefix.strip():
            raise AuthenticationConfigurationException(
                "auth.password_denied_prefixes must contain only non-empty strings"
            )
        normalized.append(prefix.strip().lower())
    return tuple(dict.fromkeys((*COMMON_PASSWORD_PREFIXES, *normalized)))


def _storage_algorithm() -> str:
    """The algorithm passwords are persisted with.

    An unknown name raises out of ``Hash.truncation_boundary`` rather than
    falling back: a typo here would silently restore the full cost bound over
    truncating storage, which is precisely the failure this module exists to
    prevent.
    """
    algorithm = config("auth.password_hash_algorithm")
    if algorithm is None:
        return Hash.DEFAULT_ALGORITHM
    if not isinstance(algorithm, str) or not algorithm.strip():
        raise AuthenticationConfigurationException(
            "auth.password_hash_algorithm must be a non-empty string"
        )
    return algorithm.strip().lower()


def _min_unique_chars() -> int:
    return _configured_positive_int(
        "auth.password_min_unique_chars",
        config("auth.password_min_unique_chars"),
        MIN_UNIQUE_CHARS,
    )


def _configured_positive_int(name: str, value: object, default: int) -> int:
    """Use the default only when absent; reject malformed security policy."""
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise AuthenticationConfigurationException(f"{name} must be a positive integer")
    return value


__all__ = [
    "COMMON_PASSWORD_PREFIXES",
    "MAX_PASSWORD_BYTES",
    "MIN_UNIQUE_CHARS",
    "check_password_strength",
    "denied_prefixes",
    "password_max_bytes",
]
