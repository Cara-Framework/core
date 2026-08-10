"""Opt-in, LOSSY projection of a payload onto JSON `number` money.

This is not a serialization boundary and it is not the wire rule. The
wire rule is ``cara.support.JsonEncoding``, which carries a ``Decimal``
as its exact digits in a string. This helper is the escape hatch a
caller reaches for *before* the boundary when its consumer contract
predates that rule and types money as a TypeScript ``number``.

It downgrades, and the downgrade is real: ``float(Decimal)`` is an
IEEE-754 double, and at cara's own ``NUMERIC(17,6)`` ceiling
``float(Decimal('99999999999.999999'))`` is ``100000000000.0`` — a penny
short of a hundred billion rounded up to exactly a hundred billion,
silently. ``QueryBuilder.aggregate_result`` carries the same incident
from the SQL side. Nothing about that arithmetic improves by living in a
helper; the helper only makes the loss a deliberate, greppable choice
instead of an accident buried in an encoder.

**It stays because deleting it would corrupt a live contract, not
because it is right.** Shipped API resources call it on the way out and
their frontends type those fields as ``number`` — and do arithmetic on
them. Flipping this function to strings would not fix the money, it
would move the breakage into a client that then coerces the string back
through the same double. The correct end state is those surfaces
adopting string money and this helper disappearing with its last caller;
that is a product change, not a framework one.

New code does not call this. It hands ``Decimal`` to the boundary and
lets ``JsonEncoding`` carry the digits.
"""

from __future__ import annotations

import math
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any


def sanitize_decimals(obj: Any) -> Any:
    """Recursively project ``Decimal`` → ``float`` and drop NaN/Inf.

    Non-finite values become ``None`` rather than the JSON-invalid
    tokens ``NaN`` / ``Infinity``: §7 says unknown is NULL, and a
    quantity that is not a number is unknown. (``JsonEncoding`` refuses
    them outright instead — a boundary has no caller to make the call
    for, so it raises rather than inventing a null.)

    ``datetime`` / ``date`` / ``time`` are normalized to ISO-8601 so a
    payload that has been through here is already in the shape the wire
    rule would produce for them; only the money differs.
    """
    if isinstance(obj, dict):
        return {k: sanitize_decimals(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [sanitize_decimals(i) for i in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, Decimal):
        f = float(obj)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, time):
        return obj.isoformat()
    return obj
