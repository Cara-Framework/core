"""
Batch Exists Validation Rule for the Cara framework.

Like ``ExistsRule``, but for array fields — runs ONE query covering every
element instead of N queries (one per index) when the wildcard syntax is
used. Use this on the array field itself (``"product_ids"``), NOT on the
wildcard (``"product_ids.*"``); applying ``exists`` per-wildcard works
but issues a separate SELECT for each element which is unacceptable for
hot endpoints.

Usage:
    "product_ids": "required|array|batch_exists:product,id"
    "category_ids": "required|array|batch_exists:category,id,is_active,1"

Semantics: validation passes only when EVERY value in the array exists in
``<table>.<column>`` (with the optional ``condition_column=condition_value``
filter). A single missing or extra value fails the field.
"""

from typing import Any, Dict, Iterable, List

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class BatchExistsRule(BaseRule):
    """Bulk DB existence check for array fields, executed in a single query.

    The implementation prefers the same model-discovery path as ``ExistsRule``
    so callers don't need to know whether the schema goes through Eloquent
    or the raw DB facade. On any infra failure (model missing, DB down) we
    fail-closed — validation rejects rather than silently passing.
    """

    def validate(self, field: str, value: Any, params: Dict[str, Any]) -> bool:
        raw = params.get("batch_exists")
        if not raw:
            return False

        # Empty / non-list values are not the concern of this rule —
        # ``required`` and ``array`` should already gate that. We pass
        # so the per-rule errors stay specific.
        if value is None:
            return True
        if not isinstance(value, (list, tuple, set)):
            return False
        if len(value) == 0:
            return True

        parts = [p.strip() for p in raw.split(",")]
        table = parts[0] if parts else ""
        column = parts[1] if len(parts) > 1 else "id"
        condition_column = parts[2] if len(parts) > 2 else None
        condition_value = parts[3] if len(parts) > 3 else None
        if not table:
            return False

        # Deduplicate the input set so the IN clause stays compact and
        # ``count == len(unique)`` is the right correctness check. The
        # caller may have a separate ``distinct`` rule; we don't depend
        # on it here.
        unique: List[Any] = list(dict.fromkeys(value))

        # 1) Try the model-class path (cleaner than raw SQL when the
        # framework already knows the table).
        try:
            from .ExistsRule import ExistsRule  # reuse model discovery

            model_class = ExistsRule()._discover_model(table)
            if model_class is not None:
                query = model_class.where_in(column, unique)
                if condition_column and condition_value is not None:
                    query = query.where(condition_column, condition_value)
                count = query.count()
                return int(count) >= len(unique)
        except Exception as exc:  # pragma: no cover - model path optional
            self._log_debug(
                f"BatchExistsRule: model-based query failed for "
                f"{table}.{column}: {exc.__class__.__name__}: {exc}"
            )

        # 2) Fall back to the DB facade with raw SELECT.
        try:
            from cara.facades import DB

            placeholders = ", ".join(["%s"] * len(unique))
            sql = f'SELECT COUNT(*) as c FROM "{table}" WHERE "{column}" IN ({placeholders})'
            params = list(unique)
            if condition_column and condition_value is not None:
                sql += f' AND "{condition_column}" = %s'
                params.append(condition_value)
            rows = DB.select(sql, params)
            count = int((rows or [{}])[0].get("c", 0))
            return count >= len(unique)
        except Exception as exc:
            self._log_debug(
                f"BatchExistsRule: DB-fallback query failed for "
                f"{table}.{column}: {exc.__class__.__name__}: {exc}"
            )

        # Fail closed if neither path resolved — silently passing on infra
        # failure would let bogus IDs through validation and surface as
        # 500s deeper in the request lifecycle.
        return False

    @staticmethod
    def _log_debug(msg: str) -> None:
        """Best-effort debug log; survives when Log facade isn't yet booted."""
        try:
            from cara.facades import Log
            Log.debug(msg, category="cara.validation.batch_exists")
        except ImportError:  # pragma: no cover
            pass

    def default_message(self, field: str, params: Dict[str, Any]) -> str:
        attribute = MessageFormatter.format_attribute_name(field)
        return f"One or more selected {attribute.lower()} are invalid."

    @staticmethod
    def _coerce_list(value: Any) -> Iterable[Any]:
        """Convenience for downstream consumers that want the deduped list."""
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
            return list(dict.fromkeys(value))
        return [value]
