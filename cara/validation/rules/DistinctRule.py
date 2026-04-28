"""Distinct rule (no duplicates within an array field). Usage: ``distinct``.

Two attachment styles supported:

  • ``items.*.id`` (wildcard) — Laravel-style: validate each sibling
    against the rest. Fails if any value is repeated.
  • ``product_ids`` (whole array) — flat-list style: validate that the
    bound value (a list/tuple) has no duplicates. Cleaner ergonomics for
    "no duplicate ids" on a top-level array of primitives, where the
    wildcard form would only be reached via ``product_ids.*`` and the
    rule below couldn't recover the parent list (the collected node is
    the list of primitives, ``leaf`` is the numeric index, the previous
    code path returned ``True`` silently).
"""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class DistinctRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        # Whole-array attachment: ``"product_ids": "...|distinct"``.
        # Value is the list itself; we just need to confirm uniqueness.
        # ``None`` is left to ``required`` / ``nullable`` to handle so
        # the message stays focused.
        if isinstance(value, (list, tuple)):
            seen: set = set()
            for item in value:
                # Hashable items get O(1) set membership. Non-hashable
                # fall back to ``count``-based check below.
                try:
                    if item in seen:
                        return False
                    seen.add(item)
                except TypeError:
                    if value.count(item) > 1:
                        return False
            return True

        data = params.get("_data", {})
        # Wildcard attachment: ``items.*.id`` or ``product_ids.*``.
        # Walk up to find the parent collection and verify uniqueness of
        # the value being validated. ``field`` is the concrete path like
        # ``items.0.id`` or ``product_ids.0``.
        segments = field.split(".")
        if len(segments) < 2:
            return True

        last = segments[-1]
        # Two shapes to handle:
        #   1. ``parent.<index>``         — flat-list wildcard, last is index.
        #   2. ``parent.<index>.<leaf>``  — list-of-objects wildcard.
        if last.isdigit():
            base_path = segments[:-1]  # ["product_ids"]
            leaf = None
        else:
            parent_path = segments[:-1]
            if not parent_path[-1].isdigit():
                return True
            base_path = parent_path[:-1]  # ["items"]
            leaf = last  # "id"

        node = data
        for p in base_path:
            if isinstance(node, dict):
                node = node.get(p)
            else:
                return True
        if not isinstance(node, list):
            return True

        collected = []
        for item in node:
            if leaf is not None and isinstance(item, dict):
                collected.append(item.get(leaf))
            else:
                collected.append(item)

        return collected.count(value) <= 1

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} field has a duplicate value."
