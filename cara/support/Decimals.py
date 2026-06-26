"""Decimal/numeric sanitization for JSON serialization."""

from __future__ import annotations

import math
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any


def sanitize_decimals(obj: Any) -> Any:
    """Recursively coerce ``Decimal`` → ``float`` and drop NaN/Inf.

    Cara's ``decimal`` cast returns ``Decimal`` instances, which the
    default JSON encoder emits as **strings** (``"99.99"``). Most
    frontend/TypeScript consumers expect ``number`` (``99.99``).

    Also normalizes datetime/date/time to ISO-8601 strings for
    consistent JSON output across all response surfaces.
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
