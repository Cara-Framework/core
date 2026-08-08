"""Return-path validation — the server side of "send me back where I was".

Every passwordless flow carries a caller-supplied destination: the page the
visitor wanted before the sign-in wall, threaded through a magic-link email,
an OAuth round trip, or a session-expiry bounce. That value is attacker
-appendable by definition, and it ends up inside a URL the product itself
emails out — so an unvalidated one turns a trusted domain into a phishing
hop (the classic open redirect, made worse here because the link arrives
from the product's own address).

The rule is deliberately absolute: an internal, root-relative path or
nothing. No host allowlist, no scheme negotiation, no "same site" parsing —
those are where open-redirect bypasses live. Frontends re-apply the same
rule before navigating; this exists so the value is already clean when it is
written into an outbound link, rather than trusting every consumer to
remember.
"""

from __future__ import annotations

MAX_LENGTH = 512


def _decodes_clean(value: str) -> bool:
    """Reject values that only become dangerous once percent-decoded.

    ``/%2F%2Fevil.example`` is a harmless-looking string that some consumers
    decode before navigating, at which point it is ``//evil.example`` — a
    protocol-relative URL. Rather than guess which consumer decodes, decode
    once here and require the result to pass the same test.
    """
    from urllib.parse import unquote

    decoded = unquote(value)
    return decoded == value or _shape_is_internal(decoded)


def _shape_is_internal(value: str) -> bool:
    if not value.startswith("/"):
        return False
    # ``//host`` is protocol-relative and ``/\host`` reaches the same place
    # because browsers normalise a backslash to a forward slash in the
    # authority position. Backslashes are refused outright rather than only
    # at index 1 — no legitimate internal route contains one, and partial
    # rules here have a long history of being walked around.
    if value.startswith("//") or "\\" in value:
        return False
    # A control character or whitespace can be stripped by a parser and shift
    # what the rest of the string means (``/\t/evil.example``).
    return all(ch > "\x20" and ch != "\x7f" for ch in value)


def is_safe(value: object) -> bool:
    """Whether ``value`` may be used as an internal redirect destination."""
    if not isinstance(value, str):
        return False
    if not value or len(value) > MAX_LENGTH:
        return False
    return _shape_is_internal(value) and _decodes_clean(value)


def safe(value: object, fallback: str = "/") -> str:
    """``value`` when it is an internal path, else ``fallback``.

    Never raises: a bad return path is a routine hostile input on a public
    endpoint, not an exceptional condition, and the caller's real work (send
    the email, complete the sign-in) must still happen.
    """
    return value if is_safe(value) else fallback  # type: ignore[return-value]


class ReturnPath:
    """Namespace form, for call sites that read better qualified."""

    MAX_LENGTH = MAX_LENGTH

    is_safe = staticmethod(is_safe)
    safe = staticmethod(safe)
