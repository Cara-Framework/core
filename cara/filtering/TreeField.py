"""Typed field vocabulary for boolean filter trees.

The facet framework in this package (``Filter`` / ``FilterSet``) models
INDEPENDENT dimensions that AND together — the storefront sidebar shape.
The tree family (``TreeField`` / ``TreeSchema`` / ``FilterTree`` /
``compile_tree``) models the other shape a data-heavy product needs: one
boolean expression over typed fields, with OR groups, that an index
endpoint accepts as a single ``filters`` parameter.

A ``TreeField`` declares ONE filterable field end-to-end:

* its ``kind`` — which fixes the operator set and value validation
* the SQL l-value it renders against (``column``), or a custom ``sql``
  hook for derived predicates that are not a bare column
* its option vocabulary (``select``), entity prefix (``entity``), or
  numeric/text bounds
* the UI metadata a frontend needs to render the field with zero
  page-side configuration (``label``, ``source``)

Fields carry NO tenant scoping and NO authorization: the compiled tree
is always ANDed inside the caller's base scope, and entity values are
resolved + authorized by the app BEFORE compilation (see
``FilterTree.entity_values`` / ``compile_tree``'s ``resolutions``).
"""

from __future__ import annotations

import datetime as _datetime
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from typing import Any

from cara.exceptions import FilterSchemaError
from cara.support import is_public_id, is_public_id_prefix

# ── field kinds ─────────────────────────────────────────────────────
KIND_SELECT = "select"  # fixed vocabulary, set membership
KIND_ENTITY = "entity"  # public-id references, resolved + authorized per value
KIND_BOOLEAN = "boolean"  # true/false
KIND_TEXT = "text"  # free text, substring/equality
KIND_NUMBER = "number"  # numeric comparisons and ranges
KIND_DATE = "date"  # calendar-day comparisons over TIMESTAMPTZ columns

KINDS = (KIND_SELECT, KIND_ENTITY, KIND_BOOLEAN, KIND_TEXT, KIND_NUMBER, KIND_DATE)

# ── operator vocabulary (wire ids) ──────────────────────────────────
# ``in`` covers the UI's ``is`` / ``is any of`` pair for set fields —
# a single value is the one-element case, so the wire stays orthogonal.
OP_IN = "in"
OP_NOT_IN = "not_in"
OP_IS = "is"
OP_IS_NOT = "is_not"
OP_CONTAINS = "contains"
OP_NOT_CONTAINS = "not_contains"
OP_GT = "gt"
OP_GTE = "gte"
OP_LT = "lt"
OP_LTE = "lte"
OP_BETWEEN = "between"
OP_BEFORE = "before"
OP_AFTER = "after"
OP_LAST_DAYS = "last_days"
OP_EMPTY = "empty"
OP_NOT_EMPTY = "not_empty"

# value-count contract per operator: (min, max) with ``None`` = unbounded
OPERATOR_ARITY: dict[str, tuple[int, int | None]] = {
    OP_IN: (1, None),
    OP_NOT_IN: (1, None),
    OP_IS: (1, 1),
    OP_IS_NOT: (1, 1),
    OP_CONTAINS: (1, 1),
    OP_NOT_CONTAINS: (1, 1),
    OP_GT: (1, 1),
    OP_GTE: (1, 1),
    OP_LT: (1, 1),
    OP_LTE: (1, 1),
    OP_BETWEEN: (2, 2),
    OP_BEFORE: (1, 1),
    OP_AFTER: (1, 1),
    OP_LAST_DAYS: (1, 1),
    OP_EMPTY: (0, 0),
    OP_NOT_EMPTY: (0, 0),
}

_KIND_OPERATORS: dict[str, tuple[str, ...]] = {
    KIND_SELECT: (OP_IN, OP_NOT_IN),
    KIND_ENTITY: (OP_IN, OP_NOT_IN),
    KIND_BOOLEAN: (OP_IS,),
    KIND_TEXT: (OP_CONTAINS, OP_NOT_CONTAINS, OP_IS, OP_IS_NOT),
    KIND_NUMBER: (OP_IS, OP_GT, OP_GTE, OP_LT, OP_LTE, OP_BETWEEN),
    KIND_DATE: (OP_BEFORE, OP_AFTER, OP_BETWEEN, OP_LAST_DAYS),
}

# ``empty`` / ``not_empty`` only exist where NULL is a real state.
_NULLABLE_OPERATORS = (OP_EMPTY, OP_NOT_EMPTY)

# Hard cap on values per condition — same defensive bound rationale as
# ``Filter._CSV_LIST_HARD_CAP``: no legitimate picker ships more.
VALUES_HARD_CAP = 100

# ``last_days`` upper bound: ten years covers every honest recency
# window while keeping the interval arithmetic bounded.
LAST_DAYS_MAX = 3650


class TreeField:
    """One filterable field of a :class:`~cara.filtering.TreeSchema.TreeSchema`.

    ``column`` is the SQL l-value the compiler renders comparisons
    against. It is APP-DECLARED, trusted text (never user input) and may
    carry ``{alias}`` placeholders resolved by the compile-time ``ctx``
    mapping, so the same schema renders inside a builder query
    (unqualified) and a hand-written aliased query (``l.status``).

    ``sql`` is the derived-predicate escape hatch: a callable
    ``(operator, values, ctx) -> (fragment, params)`` that must return a
    self-contained boolean expression. Honesty rule: the callable
    expresses a REAL row predicate — anything that cannot (a lens, an
    aggregate window) does not belong in a filter tree at all.
    """

    def __init__(
        self,
        id: str,
        kind: str,
        *,
        column: str | None = None,
        sql: Callable[[str, tuple[Any, ...], dict[str, str]], tuple[str, list[Any]]]
        | None = None,
        label: str = "",
        options: tuple[tuple[str, str], ...] = (),
        nullable: bool = False,
        prefix: str | None = None,
        source: str | None = None,
        operators: tuple[str, ...] | None = None,
        integer: bool = False,
        min_value: float | None = None,
        max_value: float | None = None,
        max_length: int = 200,
    ) -> None:
        if not id or not isinstance(id, str):
            raise FilterSchemaError("A tree field needs a non-empty string id.")
        if kind not in KINDS:
            raise FilterSchemaError(f"Unknown tree field kind {kind!r} for {id!r}.")
        if column is None and sql is None:
            raise FilterSchemaError(f"Tree field {id!r} needs a column or a sql hook.")
        if kind == KIND_SELECT and not options:
            raise FilterSchemaError(f"Select field {id!r} needs an options vocabulary.")
        if kind == KIND_ENTITY and (not prefix or not is_public_id_prefix(prefix)):
            raise FilterSchemaError(
                f"Entity field {id!r} needs a canonical public-id prefix."
            )
        allowed = _KIND_OPERATORS[kind] + (_NULLABLE_OPERATORS if nullable else ())
        if operators is not None:
            unknown = [op for op in operators if op not in allowed]
            if unknown:
                raise FilterSchemaError(
                    f"Field {id!r} declares operators {unknown!r} its kind"
                    f" {kind!r} does not support."
                )
            allowed = tuple(operators)
        self.id = id
        self.kind = kind
        self.column = column
        self.sql = sql
        self.label = label or id.replace("_", " ").title()
        self.options = tuple((str(value), str(text)) for value, text in options)
        self.nullable = bool(nullable)
        self.prefix = prefix
        self.source = source
        self.integer = bool(integer)
        self.min_value = min_value
        self.max_value = max_value
        self.max_length = int(max_length)
        self._operators = allowed
        self._option_values = frozenset(value for value, _ in self.options)

    # ── contract surface ────────────────────────────────────────────

    def allowed_operators(self) -> tuple[str, ...]:
        """The operators this field genuinely supports, in display order."""
        return self._operators

    def validate_values(self, operator: str, values: tuple[str, ...]) -> str | None:
        """Return a precise error for ``operator``/``values``, or ``None``.

        Values arrive as canonical strings (the wire keeps every value a
        string; typing is the field's job). The tree walker has already
        checked arity, so this focuses on per-kind value grammar.
        """
        if len(values) > VALUES_HARD_CAP:
            return f"'{self.id}' takes at most {VALUES_HARD_CAP} values."
        for value in values:
            if not isinstance(value, str):
                return f"'{self.id}' values must be strings."
            if not value:
                return f"'{self.id}' values must be non-empty."
            if len(value) > max(self.max_length, 64):
                return f"'{self.id}' value is too long."
        if operator in (OP_EMPTY, OP_NOT_EMPTY):
            return None
        if self.kind == KIND_SELECT:
            unknown = [value for value in values if value not in self._option_values]
            if unknown:
                return f"'{self.id}' does not accept {unknown[0]!r}."
            return None
        if self.kind == KIND_ENTITY:
            for value in values:
                if not is_public_id(value, self.prefix or ""):
                    return f"'{self.id}' expects {self.prefix} public ids."
            return None
        if self.kind == KIND_BOOLEAN:
            if values[0] not in ("true", "false"):
                return f"'{self.id}' expects true or false."
            return None
        if self.kind == KIND_TEXT:
            if len(values[0]) > self.max_length:
                return f"'{self.id}' text exceeds {self.max_length} characters."
            return None
        if self.kind == KIND_NUMBER:
            for value in values:
                error = self._validate_number(value)
                if error:
                    return error
            return None
        # KIND_DATE
        if operator == OP_LAST_DAYS:
            if not values[0].isdigit() or not 1 <= int(values[0]) <= LAST_DAYS_MAX:
                return f"'{self.id}' expects a day count between 1 and {LAST_DAYS_MAX}."
            return None
        for value in values:
            if not _is_iso_date(value):
                return f"'{self.id}' expects ISO dates (YYYY-MM-DD)."
        return None

    def _validate_number(self, value: str) -> str | None:
        try:
            number = Decimal(value)
        except InvalidOperation, TypeError, ValueError:
            return f"'{self.id}' expects numbers."
        if not number.is_finite():
            return f"'{self.id}' expects finite numbers."
        if self.integer and number != number.to_integral_value():
            return f"'{self.id}' expects whole numbers."
        if self.min_value is not None and number < Decimal(str(self.min_value)):
            return f"'{self.id}' must be at least {self.min_value}."
        if self.max_value is not None and number > Decimal(str(self.max_value)):
            return f"'{self.id}' must be at most {self.max_value}."
        return None

    def describe(self) -> dict[str, Any]:
        """JSON-serialisable field spec for the generated frontend schema."""
        spec: dict[str, Any] = {
            "id": self.id,
            "kind": self.kind,
            "label": self.label,
            "operators": list(self._operators),
            "nullable": self.nullable,
        }
        if self.options:
            spec["options"] = [
                {"value": value, "label": text} for value, text in self.options
            ]
        if self.kind == KIND_ENTITY:
            spec["prefix"] = self.prefix
        if self.source:
            spec["source"] = self.source
        if self.kind == KIND_NUMBER:
            spec["integer"] = self.integer
            if self.min_value is not None:
                spec["min"] = self.min_value
            if self.max_value is not None:
                spec["max"] = self.max_value
        return spec

    def __repr__(self) -> str:  # pragma: no cover - debugging sugar
        return f"<TreeField id={self.id!r} kind={self.kind!r}>"


def _is_iso_date(value: str) -> bool:
    """Strict ``YYYY-MM-DD`` — a datetime with a time part must NOT pass,
    because day-boundary math assumes whole calendar days."""
    if len(value) != 10:
        return False
    try:
        _datetime.date.fromisoformat(value)
    except ValueError:
        return False
    return True


__all__ = [
    "KINDS",
    "KIND_BOOLEAN",
    "KIND_DATE",
    "KIND_ENTITY",
    "KIND_NUMBER",
    "KIND_SELECT",
    "KIND_TEXT",
    "LAST_DAYS_MAX",
    "OPERATOR_ARITY",
    "OP_AFTER",
    "OP_BEFORE",
    "OP_BETWEEN",
    "OP_CONTAINS",
    "OP_EMPTY",
    "OP_GT",
    "OP_GTE",
    "OP_IN",
    "OP_IS",
    "OP_IS_NOT",
    "OP_LAST_DAYS",
    "OP_LT",
    "OP_LTE",
    "OP_NOT_CONTAINS",
    "OP_NOT_EMPTY",
    "OP_NOT_IN",
    "TreeField",
    "VALUES_HARD_CAP",
]
