"""Helpers for shaping + guarding validated request payloads.

Generic request-payload utilities every Cara app reuses:

* ``strip_none_values`` — opt out of explicit-null clearing for write
  contracts where null deliberately means "leave unchanged".
* ``validated_query_int`` — coerce a query param through ``integer|between``,
  clamping on failure.
* ``assert_editable_fields`` — mass-assignment whitelist guard; keep only
  allowed, non-``None`` keys and reject an empty result with a 422.
"""

from __future__ import annotations

from cara.exceptions.types.validation import ValidationException

# Direct submodule import (NOT ``from cara.facades import Validation``): this
# module is pulled in while ``cara.facades.__init__`` is still mid-load (a
# circular import via the HTTP stack), and at that point ``cara.facades.Validation``
# is the half-bound SUBMODULE, not the Facade class — so ``Validation.make`` blew
# up with ``module 'cara.facades.Validation' has no attribute 'make'`` on every
# ``validated_query_int`` call (recent-drops + many GET endpoints), spamming
# tracebacks. Importing the class straight from the submodule is order-independent.
from cara.facades.Validation import Validation
from cara.http.request.Request import Request


def strip_none_values(validated: dict | None) -> dict:
    """Drop ``None`` entries from a validated payload.

    ``Validation.validated()`` omits fields the caller did not send and keeps
    explicit nullable values. Use this helper only when a particular contract
    intentionally treats explicit null as "leave unchanged".
    """
    return {k: v for k, v in (validated or {}).items() if v is not None}


def validated_query_int(
    request: Request,
    key: str,
    *,
    default: int,
    lo: int,
    hi: int,
) -> int:
    """Coerce a query param via ``integer|between:lo,hi``, clamping on failure."""
    raw = request.query(key)
    value = default if raw is None or not str(raw).strip() else raw
    validator = Validation.make({key: value}, {key: f"integer|between:{lo},{hi}"})
    if validator.fails():
        try:
            return max(lo, min(hi, int(value)))
        except (TypeError, ValueError):
            return default
    return int(validator.validated()[key])


def assert_editable_fields(data: dict, allowed: set[str]) -> dict:
    """Filter ``data`` to ``allowed`` fields and raise if nothing remains.

    Mass-assignment guard for PATCH-style endpoints: drops keys not in the
    whitelist (and ``None`` values), and raises a 422 ``ValidationException``
    if the caller supplied no editable field at all.
    """
    filtered = {k: v for k, v in data.items() if k in allowed and v is not None}
    if not filtered:
        raise ValidationException.generic("No editable fields provided")
    return filtered
