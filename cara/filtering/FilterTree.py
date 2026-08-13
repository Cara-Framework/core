"""The boolean filter tree: wire grammar, validation, canonical form.

Wire shape (the ``filters`` query parameter, URL-encoded compact JSON):

    [ {"f": "status", "o": "in", "v": ["active", "draft"]},
      {"any": [ {"f": "status", "o": "in", "v": ["error"]},
                 {"f": "linked", "o": "is", "v": ["false"]} ]},
      {"f": "channel", "o": "not_in", "v": ["CHN…"]} ]

* The root is a bare array joined with AND — the "Where … And … And"
  reading — or a single-key object ``{"any": [nodes]}`` /
  ``{"all": [nodes]}`` when the ROOT connective itself is toggled
  ("Where … Or … Or"). Root-AND canonically serializes as the bare
  array; root-OR as ``{"any": […]}``.
* A group is ``{"any": [conditions]}`` (OR) or ``{"all": [conditions]}``
  (AND). Groups hold CONDITIONS ONLY: the grammar itself is the depth
  cap, and every practical query fits it. A deeper need is a grammar
  amendment, not a loosened parser.
* Every value is a STRING on the wire; the field's kind supplies typing.

Canonical form (what ``parse`` returns and ``serialize`` re-emits):

* values are trimmed; ``in`` / ``not_in`` values are deduped + sorted
  (set semantics — parity with the CSV filters this replaces)
* ``between`` bounds are sorted ascending (numerically for number
  fields, lexically for ISO dates — identical ordering)
* a single-condition group collapses to a bare condition
* NODE ORDER IS PRESERVED — it is the user's authored arrangement.
  ``canonical_key()`` sorts a throwaway clone, so cursor scopes get
  order-independent identity without reshuffling anyone's panel.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from decimal import Decimal, InvalidOperation
from typing import Any

from cara.exceptions import FilterTreeError

from .TreeCondition import TreeCondition
from .TreeField import (
    KIND_NUMBER,
    OP_BETWEEN,
    OP_IN,
    OP_NOT_IN,
    OPERATOR_ARITY,
    TreeField,
)
from .TreeGroup import GROUP_ALL, GROUP_ANY, TreeGroup
from .TreeSchema import TreeSchema

__all__ = ["FilterTree", "RAW_LENGTH_CAP"]

# The parser owns the payload ceiling so every caller—request validation,
# direct parsing, and in-memory use—enforces the same bound.
RAW_LENGTH_CAP = 4000


class FilterTree:
    """A validated, canonical filter expression bound to one schema."""

    __slots__ = ("schema", "nodes", "connective")

    def __init__(
        self,
        schema: TreeSchema,
        nodes: tuple[TreeCondition | TreeGroup, ...],
        connective: str = GROUP_ALL,
    ) -> None:
        self.schema = schema
        self.nodes = nodes
        self.connective = connective

    # ── construction ────────────────────────────────────────────────

    @classmethod
    def empty(cls, schema: TreeSchema) -> FilterTree:
        return cls(schema, ())

    @classmethod
    def parse(cls, raw: Any, schema: TreeSchema) -> FilterTree:
        """Parse + validate + canonicalize a ``filters`` payload.

        Raises :class:`~cara.exceptions.FilterTreeError` with a
        path-precise message on the FIRST problem — the payload is
        machine-built, so an invalid tree is a client bug to surface
        loudly, not to repair silently.
        """
        if raw is None or raw == "":
            return cls.empty(schema)
        if not isinstance(raw, str):
            raise FilterTreeError("filters must be a JSON string.")
        if len(raw) > RAW_LENGTH_CAP:
            raise FilterTreeError("filters payload is too large.")
        try:
            payload = json.loads(raw)
        except ValueError:
            raise FilterTreeError("filters is not valid JSON.") from None
        connective = GROUP_ALL
        if isinstance(payload, dict):
            # Root object form: exactly one of any/all holding the nodes.
            keys = set(payload)
            if keys == {GROUP_ANY}:
                connective = GROUP_ANY
                payload = payload[GROUP_ANY]
            elif keys == {GROUP_ALL}:
                payload = payload[GROUP_ALL]
            else:
                raise FilterTreeError("filters root object holds exactly one of any/all.")
        if not isinstance(payload, list):
            raise FilterTreeError("filters must be a JSON array of nodes.")
        if len(payload) > schema.max_root_nodes:
            raise FilterTreeError(
                f"filters takes at most {schema.max_root_nodes} top-level nodes."
            )
        nodes: list[TreeCondition | TreeGroup] = []
        total_conditions = 0
        for index, node in enumerate(payload):
            parsed = _parse_node(node, schema, path=f"filters[{index}]")
            total_conditions += (
                len(parsed.conditions) if isinstance(parsed, TreeGroup) else 1
            )
            if total_conditions > schema.max_conditions:
                raise FilterTreeError(
                    f"filters takes at most {schema.max_conditions} conditions."
                )
            nodes.append(parsed)
        return cls(schema, tuple(nodes), connective)

    # ── projections ─────────────────────────────────────────────────

    @property
    def is_empty(self) -> bool:
        return not self.nodes

    def conditions(self) -> Iterator[TreeCondition]:
        """Every condition in the tree, group members included."""
        for node in self.nodes:
            if isinstance(node, TreeGroup):
                yield from node.conditions
            else:
                yield node

    def without_root_conditions(self, *field_ids: str) -> FilterTree:
        """A copy with the ROOT-level conditions on ``field_ids`` dropped.

        For summary bands that break a view down BY a field: the band
        rides the view's exact tree minus the axis it tabulates, so a
        chip on that axis never zeroes the numbers that explain it.
        Only root-AND conditions are dropped — a condition inside a
        group is part of a composite branch and stripping it would
        change what the OTHER conditions in that branch mean, so groups
        ride through untouched. Callers on a root-ANY tree get the same
        conservative treatment: dropped conditions widen nothing there,
        because remaining branches still gate every row.
        """
        wanted = set(field_ids)
        kept = tuple(
            node
            for node in self.nodes
            if isinstance(node, TreeGroup) or node.field not in wanted
        )
        if len(kept) == len(self.nodes):
            return self
        return FilterTree(self.schema, kept, self.connective)

    def entity_values(self) -> dict[str, tuple[str, ...]]:
        """Public ids referenced per entity field, deduped + sorted.

        The app resolves AND AUTHORIZES each id before compilation —
        wherever the id sits in the tree: naming an entity inside an OR
        branch still reveals that it exists.
        """
        values: dict[str, set[str]] = {}
        for condition in self.conditions():
            field = self.schema.field(condition.field)
            if field is not None and field.kind == "entity":
                values.setdefault(condition.field, set()).update(condition.values)
        return {field_id: tuple(sorted(ids)) for field_id, ids in sorted(values.items())}

    # ── serial forms ────────────────────────────────────────────────

    def serialize(self) -> str:
        """Canonical wire form, node order preserved ("" when empty).

        Root-AND stays the bare array (byte-stable with every existing
        URL); root-OR wraps as ``{"any": […]}``."""
        if self.is_empty:
            return ""
        wires = [node.to_wire() for node in self.nodes]
        if self.connective == GROUP_ANY:
            return json.dumps({GROUP_ANY: wires}, separators=(",", ":"))
        return json.dumps(wires, separators=(",", ":"))

    def canonical_key(self) -> str:
        """Order-independent identity for cursor scopes and cache keys."""
        if self.is_empty:
            return ""
        wires = [
            node.to_wire()
            for node in sorted(self.nodes, key=lambda node: node.sort_key())
        ]
        for wire in wires:
            for connective in (GROUP_ANY, GROUP_ALL):
                if connective in wire:
                    wire[connective] = sorted(
                        wire[connective],
                        key=lambda c: json.dumps(
                            c, sort_keys=True, separators=(",", ":")
                        ),
                    )
        return json.dumps({self.connective: wires}, sort_keys=True, separators=(",", ":"))


# ── node parsing ────────────────────────────────────────────────────


def _parse_node(node: Any, schema: TreeSchema, *, path: str) -> TreeCondition | TreeGroup:
    if not isinstance(node, dict):
        raise FilterTreeError(f"{path}: a node must be an object.")
    is_any = GROUP_ANY in node
    is_all = GROUP_ALL in node
    if is_any or is_all:
        if len(node) != 1:
            raise FilterTreeError(f"{path}: a group holds exactly one of any/all.")
        connective = GROUP_ANY if is_any else GROUP_ALL
        children = node[connective]
        if not isinstance(children, list) or not children:
            raise FilterTreeError(f"{path}: a group needs at least one condition.")
        if len(children) > schema.max_group_children:
            raise FilterTreeError(
                f"{path}: a group takes at most {schema.max_group_children} conditions."
            )
        conditions = tuple(
            _parse_condition(child, schema, path=f"{path}.{connective}[{index}]")
            for index, child in enumerate(children)
        )
        if len(conditions) == 1:
            # ``any``/``all`` of one condition IS that condition.
            return conditions[0]
        return TreeGroup(connective, conditions)
    return _parse_condition(node, schema, path=path)


def _parse_condition(node: Any, schema: TreeSchema, *, path: str) -> TreeCondition:
    if not isinstance(node, dict):
        raise FilterTreeError(f"{path}: a condition must be an object.")
    if GROUP_ANY in node or GROUP_ALL in node:
        raise FilterTreeError(f"{path}: groups cannot nest inside groups.")
    extra = set(node) - {"f", "o", "v"}
    if extra:
        raise FilterTreeError(f"{path}: unknown key {sorted(extra)[0]!r}.")
    field_id = node.get("f")
    operator = node.get("o")
    raw_values = node.get("v", [])
    if not isinstance(field_id, str) or not field_id:
        raise FilterTreeError(f"{path}: 'f' must be a field id.")
    field = schema.field(field_id)
    if field is None:
        raise FilterTreeError(f"{path}: unknown field {field_id!r}.")
    if not isinstance(operator, str) or operator not in OPERATOR_ARITY:
        raise FilterTreeError(f"{path}: unknown operator {operator!r}.")
    if operator not in field.allowed_operators():
        raise FilterTreeError(
            f"{path}: field {field_id!r} does not support {operator!r}."
        )
    if not isinstance(raw_values, list):
        raise FilterTreeError(f"{path}: 'v' must be an array of strings.")
    if any(not isinstance(value, str) for value in raw_values):
        raise FilterTreeError(f"{path}: 'v' must contain strings only.")
    values = _canonical_values(field, operator, tuple(v.strip() for v in raw_values))
    lo, hi = OPERATOR_ARITY[operator]
    if len(values) < lo or (hi is not None and len(values) > hi):
        if hi == lo:
            expected = str(lo)
        elif hi is None:
            expected = f"at least {lo}"
        else:
            expected = f"{lo}..{hi}"
        raise FilterTreeError(f"{path}: operator {operator!r} takes {expected} value(s).")
    error = field.validate_values(operator, values)
    if error:
        raise FilterTreeError(f"{path}: {error}")
    return TreeCondition(field_id, operator, values)


def _canonical_values(
    field: TreeField, operator: str, values: tuple[str, ...]
) -> tuple[str, ...]:
    if operator in (OP_IN, OP_NOT_IN):
        return tuple(sorted({value for value in values if value}))
    if operator == OP_BETWEEN and len(values) == 2 and all(values):
        if field.kind == KIND_NUMBER:
            try:
                return tuple(sorted(values, key=Decimal))
            except InvalidOperation:
                return values  # validate_values reports the real error
        return tuple(sorted(values))
    return values
