"""The key form of one idempotency parameter value.

A job's idempotency key hashes its parameters as JSON, so every value needs
one stable, JSON-native spelling. Before 2026-11-17 anything that was not a
JSON scalar, a container or a model (``.id``) normalized to ``None`` and
silently dropped out of the key: a ``Decimal`` price, a date window or a UUID.
Two dispatches that differed only there shared one key — the second waited on
the first and settled as done without its own work, or answered from the
first one's cached result. Sets iterated in per-process hash order, so the
same set keyed differently on every worker.

``strict`` is for a value the job DECLARED as identity (``idempotency_params``):
a value with no key form fails the key rather than leaving it. The lenient
full-attribute scan keeps dropping such values — an incidental attribute that
holds a service is not identity.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

__all__ = ["normalize_idempotency_param"]


def normalize_idempotency_param(value: Any, *, strict: bool = False) -> Any:
    """``value`` as a stable JSON-native shape; ``None`` drops it from the key."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Enum):
        return normalize_idempotency_param(value.value, strict=strict)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [
            item
            for item in (normalize_idempotency_param(v, strict=strict) for v in value)
            if item is not None
        ]
        if isinstance(value, (set, frozenset)):
            items.sort(key=lambda item: json.dumps(item, sort_keys=True))
        return items
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key in sorted(value.keys(), key=str):
            item = normalize_idempotency_param(value[key], strict=strict)
            if item is not None:
                normalized[str(key)] = item
        return normalized
    if hasattr(value, "id"):
        return value.id
    if hasattr(value, "public_id"):
        return value.public_id
    if not strict:
        return None
    raise TypeError(
        f"idempotency parameter of type {type(value).__name__} has no stable key "
        "form; store it as a JSON-native value or leave it out of "
        "``idempotency_params``."
    )
