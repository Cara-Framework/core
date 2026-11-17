"""One definition of "this value carries a person's identity".

An append-only ledger (an audit trail whose table trigger refuses UPDATE and
DELETE) can never be redacted after the fact, so whatever it stores outlives
any deletion request. Every such door applies ONE rule: an e-mail address or
an IP address — as a value, as a dict KEY, or under a well-known identity key
name — is personal data.

Two ways to apply it: :func:`contains_private_identity` for a door that
refuses the write (fail closed), :func:`redact_private_identity` for a door
that must keep the record but not the identity (the field names stay, the
values become :data:`REDACTED`).
"""

from __future__ import annotations

import ipaddress
import re
from typing import Any

PII_KEYS = frozenset({"email", "email_address", "ip", "ip_address"})
REDACTED = "[redacted]"

_EMAIL = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
#: An address inside free text ("contact bob@example.com"), for the scalar
#: rules below: a note that quotes an e-mail carries it just the same.
_EMAIL_IN_TEXT = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_MAX_DEPTH = 12


def looks_like_network_identity(value: Any) -> bool:
    """Is this scalar an e-mail address or an IP address?"""
    if not isinstance(value, str):
        return False
    normalized = value.strip()
    if not normalized:
        return False
    if _EMAIL.fullmatch(normalized):
        return True
    try:
        ipaddress.ip_address(normalized)
    except ValueError:
        return False
    return True


def _pii_key(key: Any) -> bool:
    text = str(key)
    return text.strip().lower() in PII_KEYS or looks_like_network_identity(text)


def contains_private_identity(value: Any, *, _depth: int = 0) -> bool:
    """Does ``value`` carry a person's identity anywhere inside it?

    Too deep to inspect counts as carrying one: the door fails closed.
    """
    if _depth > _MAX_DEPTH:
        return True
    if isinstance(value, dict):
        return any(
            _pii_key(key) or contains_private_identity(child, _depth=_depth + 1)
            for key, child in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(contains_private_identity(c, _depth=_depth + 1) for c in value)
    return looks_like_network_identity(value) or (
        isinstance(value, str) and _EMAIL_IN_TEXT.search(value) is not None
    )


def redact_private_identity(value: Any, *, _depth: int = 0) -> Any:
    """``value`` with every identity replaced by :data:`REDACTED`.

    A value under an identity key is redacted whatever it is; a key that is
    itself an e-mail/IP becomes :data:`REDACTED` (numbered when several
    collide). Too deep to inspect is redacted whole.
    """
    if _depth > _MAX_DEPTH:
        return REDACTED
    if isinstance(value, dict):
        out: dict[Any, Any] = {}
        for key, child in value.items():
            if str(key).strip().lower() in PII_KEYS:
                out[key] = None if child is None else REDACTED
                continue
            if looks_like_network_identity(str(key)):
                name = REDACTED
                index = 1
                while name in out:
                    index += 1
                    name = f"{REDACTED}#{index}"
                out[name] = redact_private_identity(child, _depth=_depth + 1)
                continue
            out[key] = redact_private_identity(child, _depth=_depth + 1)
        return out
    if isinstance(value, list):
        return [redact_private_identity(c, _depth=_depth + 1) for c in value]
    if isinstance(value, tuple):
        return tuple(redact_private_identity(c, _depth=_depth + 1) for c in value)
    if looks_like_network_identity(value):
        return REDACTED
    if isinstance(value, str):
        return _EMAIL_IN_TEXT.sub(REDACTED, value)
    return value


__all__ = [
    "PII_KEYS",
    "REDACTED",
    "contains_private_identity",
    "looks_like_network_identity",
    "redact_private_identity",
]
