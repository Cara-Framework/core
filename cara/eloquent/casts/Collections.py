"""
Collection Cast Types for Cara ORM

Handles arrays, lists, and Cara Collection objects.
"""

from __future__ import annotations

from cara.support import json_dumps


def _write_list(value, *, cast_name: str):
    """Encode a list without fabricating an empty collection for bad input."""
    if value is None:
        return None
    if not isinstance(value, list):
        raise TypeError(f"{cast_name} requires a list or None")
    return json_dumps(value)
