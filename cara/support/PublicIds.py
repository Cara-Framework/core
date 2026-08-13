"""Canonical public-id shape validation.

``MakesPublicId`` GENERATES ``PREFIX + 26-char ULID`` identifiers; this
module is the matching single source for VALIDATING that shape. The
``public_id_csv`` validation rule and the filter-tree entity fields both
check tokens through here, so the accepted grammar can never drift
between the two doors.
"""

from __future__ import annotations

import re

__all__ = ["is_public_id", "is_public_id_prefix"]

# A prefix is 2-10 chars, uppercase alphanumeric, starting with a letter
# (``CHN``, ``SVW``…). Kept identical to the historical rule grammar.
_PREFIX = re.compile(r"[A-Z][A-Z0-9]{1,9}")

# Crockford base32 as ULIDs use it: no I, L, O, U.
_ULID = re.compile(r"[0-9A-HJKMNP-TV-Z]{26}")


def is_public_id_prefix(prefix: str) -> bool:
    """Whether ``prefix`` is a well-formed public-id prefix."""
    return isinstance(prefix, str) and _PREFIX.fullmatch(prefix) is not None


def is_public_id(value: str, prefix: str) -> bool:
    """Whether ``value`` is exactly ``prefix`` + a canonical ULID.

    Strict on purpose: no surrounding whitespace, no case-folding — a
    truncated or hand-typed id must NOT pass and then leak into an
    entity lookup as a plausible-but-wrong key.
    """
    if not isinstance(value, str) or not is_public_id_prefix(prefix):
        return False
    return value.startswith(prefix) and _ULID.fullmatch(value[len(prefix) :]) is not None
