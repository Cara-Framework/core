"""Pure value canonicalizers for filtering contracts."""

from __future__ import annotations

from typing import Any


def csv_filter_values(value: Any) -> list[str] | None:
    """Return the canonical token set behind a multi-value CSV filter.

    ``status=active,%20draft,active`` and ``status=active,draft`` describe
    the same filter. Trimming, de-duplicating, and sorting here gives query
    builders, cache keys, and audit records one spelling of that intent.

    ``None`` represents an absent or all-whitespace filter. Validation rules
    decide whether tokens are acceptable; this pure helper only canonicalizes
    tokens after validation. It must not be used where order or repetition is
    meaningful.
    """
    raw = "" if value is None else str(value)
    values = sorted({token.strip() for token in raw.split(",") if token.strip()})
    return values or None


__all__ = ["csv_filter_values"]
