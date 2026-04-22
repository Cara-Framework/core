"""Distinct rule (no duplicates within an array field). Usage: ``distinct``.

Works in combination with array wildcard rules (``items.*.id``) — Laravel
applies it to each sibling position and fails if duplicates are present.
"""
from typing import Any, Dict

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class DistinctRule(BaseRule):
    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        data = params.get("_data", {})
        # Expected to be used alongside a wildcard (items.*). Walk up to
        # find the parent collection and verify uniqueness of the value.
        # field is the concrete path like "items.0.id".
        segments = field.split(".")
        if len(segments) < 2:
            return True

        # Collect sibling values using all but the last segment group.
        # For ``items.0.id`` siblings live at ``items.*.id``.
        parent_path = segments[:-1]  # e.g. ["items", "0"]
        # Replace the numeric index with * and walk.
        if not parent_path[-1].isdigit():
            return True
        base_path = parent_path[:-1]  # ["items"]
        leaf = segments[-1]  # "id"

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
            if isinstance(item, dict):
                collected.append(item.get(leaf))
            else:
                collected.append(item)

        return collected.count(value) <= 1

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attr = MessageFormatter.format_attribute_name(field)
        return f"The {attr.lower()} field has a duplicate value."
