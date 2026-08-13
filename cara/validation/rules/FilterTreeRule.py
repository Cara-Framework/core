"""Boolean filter-tree rule. Usage: ``filter_tree:<schema_name>``."""

from __future__ import annotations

from typing import Any

from cara.exceptions import FilterTreeError
from cara.validation.rules.BaseRule import BaseRule

_COMPOSITES = (list, tuple, set, frozenset, dict, bytes, bytearray)


class FilterTreeRule(BaseRule):
    """Validate a ``filters`` payload against a registered tree schema.

    The heavy lifting lives in ``cara.filtering.FilterTree.parse`` —
    the SAME code path the controller re-parses through afterwards, so
    the rule can never accept a payload the compiler would reject.
    ``validate`` stashes the parser's path-precise message in ``params``
    (the validator hands the same dict to ``message``), so a failing
    payload reports WHAT was wrong, not just that something was.
    """

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        from cara.filtering import (  # local: cycle with cara.filtering
            FilterTree,
            tree_schema,
        )

        if value is None or isinstance(value, _COMPOSITES):
            return False
        schema = tree_schema(str(params.get("filter_tree") or ""))
        if schema is None:
            params["_filter_tree_error"] = (
                f"'{field}' references an unregistered filter schema."
            )
            return False
        try:
            FilterTree.parse(str(value), schema)
        except FilterTreeError as exc:
            params["_filter_tree_error"] = f"'{field}': {exc}"
            return False
        return True

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        return str(
            params.get("_filter_tree_error")
            or f"'{field}' must be a valid filter expression."
        )
