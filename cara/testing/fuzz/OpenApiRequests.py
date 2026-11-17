"""Hypothesis strategies that derive whole requests from an OpenAPI document."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote

from hypothesis import strategies as st

from cara.testing.fuzz.FuzzRequest import FuzzRequest
from cara.testing.fuzz.HostileValues import HostileValues
from cara.testing.fuzz.IdPools import IdPools
from cara.testing.fuzz.OpenApiOperation import OpenApiOperation

#: Query keys every list route plausibly reads even when the spec declares
#: none (Cara's generated spec omits query parameters read in the handler).
GENERIC_QUERY_KEYS = (
    "page",
    "per_page",
    "limit",
    "offset",
    "sort",
    "order",
    "direction",
    "sort_by",
    "cursor",
    "include",
    "fields",
    "search",
    "q",
)


class OpenApiRequests:
    """Strategies for one spec: ``request(op)`` is the entry point.

    ``pools`` supplies real ids by parameter name; ``generic_query_keys`` the
    keys tried on every operation beyond the declared ones (a product adds its
    own list vocabulary — ``category``, ``min_price`` — here).
    """

    def __init__(
        self,
        spec: dict,
        pools: IdPools | None = None,
        *,
        generic_query_keys: tuple[str, ...] = GENERIC_QUERY_KEYS,
    ) -> None:
        self.spec = spec
        self.pools = pools or IdPools()
        self.generic_query_keys = generic_query_keys
        self.filter_schemas: dict = spec.get("x-filter-schemas") or {}

    # ── schema plumbing ──────────────────────────────────────────────────
    def resolve(self, schema: dict | None) -> dict:
        """Follow ``#/...`` references (bounded) to the schema they name."""
        seen = 0
        schema = schema or {}
        while "$ref" in schema and seen < 20:
            node: Any = self.spec
            for part in schema["$ref"].lstrip("#/").split("/"):
                node = node.get(part, {}) if isinstance(node, dict) else {}
            schema = node or {}
            seen += 1
        return schema

    # ── valid values ─────────────────────────────────────────────────────
    def valid_value(self, schema: dict, name: str, depth: int = 0) -> st.SearchStrategy:
        """A value the schema accepts (real ids where the name asks for one)."""
        schema = self.resolve(schema)
        if not schema or depth > 5:
            return st.sampled_from(["x", 1, True])
        if "allOf" in schema:
            merged: dict = {}
            for branch in schema["allOf"]:
                merged.update(self.resolve(branch))
            merged.update({k: v for k, v in schema.items() if k != "allOf"})
            return self.valid_value(merged, name, depth + 1)
        for key in ("oneOf", "anyOf"):
            if key in schema:
                return st.one_of(
                    *[self.valid_value(b, name, depth + 1) for b in schema[key]]
                )
        if "enum" in schema:
            return st.sampled_from(schema["enum"])
        strategy = self._typed(schema, name, depth)
        return strategy | st.none() if schema.get("nullable") else strategy

    def _typed(self, schema: dict, name: str, depth: int) -> st.SearchStrategy:
        kind = schema.get("type")
        if kind == "string":
            return self._string(schema, name)
        if kind in ("integer", "number"):
            pool = self.pools.for_name(name)
            if pool and kind == "integer":
                return st.sampled_from([int(v) for v in pool if str(v).isdigit()] or [1])
            low, high = schema.get("minimum"), schema.get("maximum")
            if kind == "integer":
                return st.integers(
                    min_value=None if low is None else int(low),
                    max_value=None if high is None else int(high),
                )
            return st.floats(
                min_value=low, max_value=high, allow_nan=False, allow_infinity=False
            )
        if kind == "boolean":
            return st.booleans()
        if kind == "array":
            item = self.valid_value(schema.get("items", {}), name, depth + 1)
            low = schema.get("minItems", 0)
            return st.lists(
                item, min_size=low, max_size=min(schema.get("maxItems", 4), low + 4)
            )
        if kind == "object" or "properties" in schema:
            return self.valid_object(schema, depth)
        return st.sampled_from(["x", 1, True])

    def _string(self, schema: dict, name: str) -> st.SearchStrategy:
        pool = self.pools.for_name(name)
        if pool:
            return st.sampled_from(pool)
        fmt = schema.get("format")
        formats = {
            "date": lambda: st.dates().map(lambda d: d.isoformat()),
            "date-time": lambda: st.datetimes().map(lambda d: d.isoformat() + "Z"),
            "email": st.emails,
            "uri": lambda: st.just("https://example.test/a"),
            "url": lambda: st.just("https://example.test/a"),
            "uuid": lambda: st.uuids().map(str),
        }
        if fmt in formats:
            return formats[fmt]()
        if schema.get("pattern"):
            try:
                return st.from_regex(schema["pattern"], fullmatch=True).filter(
                    lambda s: len(s) <= schema.get("maxLength", 2000)
                )
            except Exception:  # noqa: BLE001 - a JS-only regex falls back to free text.
                pass
        low = schema.get("minLength", 0)
        edges = [
            st.text(min_size=low, max_size=min(schema.get("maxLength", 40), low + 40))
        ]
        if schema.get("maxLength"):  # the exact boundary, in the widest characters
            edges += [
                st.just("𝒳" * schema["maxLength"]),
                st.just("a" * schema["maxLength"]),
            ]
        return st.one_of(*edges)

    def valid_object(self, schema: dict, depth: int = 0) -> st.SearchStrategy:
        properties = schema.get("properties", {})
        required = {
            name: self.valid_value(properties[name], name, depth + 1)
            for name in schema.get("required", [])
            if name in properties
        }
        optional = {
            name: self.valid_value(item, name, depth + 1)
            for name, item in properties.items()
            if name not in required
        }
        return st.fixed_dictionaries(required, optional=optional)

    # ── request parts ────────────────────────────────────────────────────
    def path(self, op: OpenApiOperation, *, real_ids: bool) -> st.SearchStrategy:
        """Fill the holes with real ids (``real_ids``, when pooled) or malformed segments."""
        names = op.path_params
        if not names:
            return st.just(op.path)

        def segment(name: str) -> st.SearchStrategy:
            pool = self.pools.for_name(name)
            if real_ids and pool:
                return st.sampled_from([quote(v, safe="") for v in pool])
            return st.sampled_from(HostileValues.PATH_SEGMENTS)

        def fill(values: tuple) -> str:
            out = op.path
            for name, value in zip(names, values, strict=True):
                out = out.replace("{" + name + "}", value)
            return out

        return st.tuples(*[segment(n) for n in names]).map(fill)

    def query(self, op: OpenApiOperation) -> st.SearchStrategy:
        """Declared query parameters (valid or hostile) plus generic keys."""
        declared = [
            self._query_parameter(p, op)
            for p in op.operation.get("parameters", [])
            if p.get("in") == "query"
        ]
        generic = st.lists(
            st.tuples(
                st.sampled_from(self.generic_query_keys), HostileValues.query_value()
            ),
            max_size=2,
        )

        def assemble(parts: tuple) -> bytes:
            flat: list[tuple[str, Any]] = [
                pair for part in parts[0] if part for pair in part
            ]
            flat.extend(parts[1])
            return "&".join(self._pair(k, v) for k, v in flat).encode("ascii")

        composed = st.tuples(
            st.tuples(*declared) if declared else st.just(()), generic
        ).map(assemble)
        return st.one_of(
            composed, composed, composed, st.sampled_from(HostileValues.RAW_QUERIES)
        )

    def _query_parameter(
        self, parameter: dict, op: OpenApiOperation
    ) -> st.SearchStrategy:
        name = parameter["name"]
        if name == "filters" and op.operation_id in self.filter_schemas:
            good = self.filter_tree(op.operation_id)
        else:
            good = self.valid_value(parameter.get("schema", {}), name).filter(
                lambda v: v is not None
            )
        return st.one_of(
            st.none(),
            good.map(lambda v: [(name, str(v).lower() if isinstance(v, bool) else v)]),
            HostileValues.query_value().map(lambda v: [(name, v)]),
            st.sampled_from(
                [
                    [(f"{name}[]", "1"), (f"{name}[]", "2")],
                    [(f"{name}[a]", "1")],
                    [(name, "1"), (name, "2")],
                    [(name, "")],
                ]
            ),
        )

    def filter_tree(self, operation_id: str) -> st.SearchStrategy:
        """A well-formed filter tree over ``x-filter-schemas`` fields, values arbitrary."""
        fields = self.filter_schemas[operation_id]["fields"]

        def condition(item: dict) -> st.SearchStrategy:
            values = [st.lists(HostileValues.query_value(), max_size=3)]
            if item.get("options"):
                choices = [
                    o.get("value", o.get("id")) if isinstance(o, dict) else o
                    for o in item["options"]
                ]
                values.append(st.lists(st.sampled_from(choices), min_size=1, max_size=3))
            pool = self.pools.for_name(item["id"])
            if item.get("kind") == "entity" and pool:
                values.append(st.lists(st.sampled_from(pool), min_size=1, max_size=3))
            return st.fixed_dictionaries(
                {
                    "f": st.just(item["id"]),
                    "o": st.sampled_from(item["operators"]),
                    "v": st.one_of(*values),
                }
            )

        node = st.sampled_from(fields).flatmap(condition)
        tree = st.lists(
            node | st.lists(node, min_size=1, max_size=3).map(lambda n: {"any": n}),
            min_size=1,
            max_size=4,
        )
        broken = HostileValues.scalar().map(lambda v: {"f": v, "o": v, "v": v})
        return st.one_of(
            tree.map(json.dumps),
            st.lists(broken, max_size=2).map(json.dumps),
            HostileValues.query_value(),
        )

    def body(self, op: OpenApiOperation) -> st.SearchStrategy:
        """``(content_type, bytes, note)`` for the operation's request body."""
        if op.method == "GET":
            return st.just((None, b"", "no-body"))
        content = (op.operation.get("requestBody") or {}).get("content", {})
        schema = self.resolve((content.get("application/json") or {}).get("schema"))
        raw = st.tuples(
            st.sampled_from(HostileValues.RAW_CONTENT_TYPES),
            st.sampled_from(HostileValues.RAW_BODIES),
            st.just("raw-body"),
        )
        if not schema.get("properties"):
            return st.one_of(st.just(("application/json", b"", "no-body")), raw)
        valid = self.valid_object(schema)
        as_valid = valid.map(lambda b: ("application/json", self._dumps(b), "valid-body"))
        as_mutated = valid.flatmap(HostileValues.mutate_object).map(
            lambda b: ("application/json", self._dumps(b), "mutated-body")
        )
        as_scalar = HostileValues.scalar().map(
            lambda v: ("application/json", self._dumps(v), "scalar-body")
        )
        return st.one_of(as_valid, as_mutated, as_mutated, raw, as_scalar)

    def request(
        self, op: OpenApiOperation, *, real_ids: bool = True
    ) -> st.SearchStrategy:
        """A complete :class:`FuzzRequest` for one operation."""

        def build(parts: tuple) -> FuzzRequest:
            path, query, (content_type, body, note) = parts
            return FuzzRequest(
                method=op.method,
                path=path,
                query=query,
                body=body,
                content_type=content_type,
                note=note,
            )

        return st.tuples(
            self.path(op, real_ids=real_ids), self.query(op), self.body(op)
        ).map(build)

    def headers(self, names: tuple[str, ...]) -> st.SearchStrategy:
        """A dict giving one to all of ``names`` a hostile, sendable value."""
        return st.dictionaries(
            st.sampled_from(names), HostileValues.header_value(), min_size=1
        )

    @staticmethod
    def _pair(key: str, value: Any) -> str:
        return (
            f"{quote(key, safe='[]', errors='surrogatepass')}="
            f"{quote(str(value), safe='', errors='surrogatepass')}"
        )

    @staticmethod
    def _dumps(value: Any) -> bytes:
        return json.dumps(value, ensure_ascii=True, allow_nan=True).encode("utf-8")
