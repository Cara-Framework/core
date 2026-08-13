"""Best-effort JSON-schema inference for serializer payload expressions.

A ``JsonResource`` builds its wire payload as a runtime dict, so there is no
static return type to read. What IS static is the shape of the *expression*
behind each key, and the framework owns the vocabulary those expressions are
written in: the ``opt_*`` coercion helpers from
``cara.http.resources.Serialization`` and the ``Resource(...)`` /
``Resource.collection(...)`` composition contract of ``JsonResource``.

This module turns such an expression into a JSON-schema fragment. It never
fabricates a type it cannot see: anything not self-describing degrades to the
permissive ``{}``, which honestly means "this key exists, type unknown".
"""

from __future__ import annotations

import ast
from typing import Any

# ``opt_*`` helper name -> JSON-schema type. Exactly the helpers
# ``cara.http.resources.Serialization`` defines: the same wrapper means the same
# wire type in every application, because the framework is the one coercing.
# An application that adds its own helper registers it — the framework never
# guesses at a name it does not own.
OPT_TYPE: dict[str, dict[str, Any]] = {
    "opt_int": {"type": "integer", "nullable": True},
    "opt_float": {"type": "number", "nullable": True},
    "opt_bool": {"type": "boolean", "nullable": True},
    "opt_str": {"type": "string", "nullable": True},
    "opt_datetime": {"type": "string", "format": "date-time", "nullable": True},
    "opt_list": {"type": "array", "items": {}, "nullable": True},
}

# Builtin cast name -> JSON-schema type.
CAST_TYPE: dict[str, dict[str, Any]] = {
    "int": {"type": "integer"},
    "float": {"type": "number"},
    "bool": {"type": "boolean"},
    "str": {"type": "string"},
}

# Every call whose NAME alone settles the wire type of its result. Applications
# extend this with their own coercion helpers.
CALL_TYPE: dict[str, dict[str, Any]] = {**OPT_TYPE, **CAST_TYPE}

# A wholly-permissive schema: "this field exists, type unknown". Honest stand-in
# for values we cannot derive statically (a bare ORM read / helper call).
ANY_SCHEMA: dict[str, Any] = {}

# Wrapper calls that pass the real payload dict through unchanged; the dict we
# want is their first positional argument.
PASSTHROUGH_WRAPPERS = frozenset(
    {
        "normalize",
        "_sanitize",
        "_filter_missing",
    }
)

# Serializer methods a resource class exposes, and the calls that mean "this
# payload continues one produced elsewhere" (a parent class or a model), whose
# keys are therefore not statically knowable.
SERIALIZER_METHODS = ("to_array", "to_dict")
DYNAMIC_BASE_CALLS = ("serialize", "normalize")


def resource_ref(name: str) -> dict[str, Any]:
    """A ``$ref`` at the component schema emitted for ``name``."""
    return {"$ref": f"#/components/schemas/{name}"}


def const_schema(node: ast.Constant) -> dict[str, Any]:
    """Schema for a literal constant; ``None`` carries no type information."""
    value = node.value
    if value is None:
        return dict(ANY_SCHEMA)
    if isinstance(value, bool):
        return {"type": "boolean"}
    if isinstance(value, int):
        return {"type": "integer"}
    if isinstance(value, float):
        return {"type": "number"}
    if isinstance(value, str):
        # A literal in a wire resource is a discriminator/value contract, not
        # merely an arbitrary string. OpenAPI 3.0 represents that with a
        # singleton enum (``const`` only arrived in later JSON Schema dialects).
        return {"type": "string", "enum": [value]}
    return dict(ANY_SCHEMA)


def _callee_name(func: ast.AST) -> str | None:
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def infer_value_schema(
    node: ast.AST,
    locals_map: dict[str, dict[str, Any]] | None = None,
    call_types: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Best-effort JSON-schema for a payload key's value EXPRESSION.

    Self-describing expressions (``opt_*`` wrappers, casts, literals, nested
    resource references) yield a precise-ish type; everything else yields the
    permissive ``{}`` (any).

    ``locals_map`` resolves bare ``Name`` references to a type derived from an
    earlier ``name = self.opt_float(...)`` style binding in the same method —
    the pattern where every key is precomputed into a local and the returned
    dict literal just references it.

    ``call_types`` is the name-settles-the-type table; it defaults to the
    framework's own :data:`CALL_TYPE` and an application widens it with its own
    coercion helpers.
    """
    locals_map = locals_map or {}
    call_types = CALL_TYPE if call_types is None else call_types
    # Bare ``amount`` / ``rating`` — resolve from an earlier typed binding.
    if isinstance(node, ast.Name):
        return dict(locals_map.get(node.id, ANY_SCHEMA))
    # ``self.opt_float(x)`` / ``self.opt_int(x)`` / ... — the dominant pattern.
    if isinstance(node, ast.Call):
        func = node.func
        # ``self.opt_xxx(...)`` or bare ``opt_xxx(...)``.
        attr = _callee_name(func)
        if attr in call_types:
            return dict(call_types[attr])
        # ``SomeResource.collection(...)`` -> array of that resource.
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "collection"
            and (isinstance(func.value, ast.Name) and func.value.id.endswith("Resource"))
        ):
            return {"type": "array", "items": resource_ref(func.value.id)}
        # ``SomeResource(x).to_array()`` / ``.to_dict()`` -> that resource.
        if isinstance(func, ast.Attribute) and func.attr in SERIALIZER_METHODS:
            inner = func.value
            if (
                isinstance(inner, ast.Call)
                and isinstance(inner.func, ast.Name)
                and inner.func.id.endswith("Resource")
            ):
                return resource_ref(inner.func.id)
        return dict(ANY_SCHEMA)
    if isinstance(node, ast.Constant):
        return const_schema(node)
    if isinstance(node, (ast.List, ast.Tuple)):
        return {"type": "array", "items": ANY_SCHEMA}
    if isinstance(node, ast.Dict):
        properties: dict[str, Any] = {}
        required: list[str] = []
        open_schema = False
        for key, value in zip(node.keys, node.values, strict=False):
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                properties[key.value] = infer_value_schema(
                    value,
                    locals_map,
                    call_types,
                )
                required.append(key.value)
            else:
                # ``{**bundle}`` / a computed key carries members this AST
                # cannot name. Keep the visible literal keys, but stay open.
                open_schema = True
        schema: dict[str, Any] = {
            "type": "object",
            "properties": properties,
        }
        if required:
            schema["required"] = required
        if open_schema:
            schema["additionalProperties"] = True
            schema["x-fields-partial"] = True
        return schema
    if isinstance(node, ast.DictComp):
        return {"type": "object", "additionalProperties": True}
    if isinstance(node, ast.ListComp):
        return {"type": "array", "items": ANY_SCHEMA}
    # ``a if cond else b`` — unify the two branches.
    if isinstance(node, ast.IfExp):
        # ``None`` is not an unknowable expression here; it is an explicit
        # nullable branch. Preserve the other branch's registered helper/ref
        # schema instead of letting the generic ``{}`` sentinel erase it.
        if _is_none_literal(node.body):
            return _nullable(
                infer_value_schema(node.orelse, locals_map, call_types),
            )
        if _is_none_literal(node.orelse):
            return _nullable(
                infer_value_schema(node.body, locals_map, call_types),
            )
        return unify(
            infer_value_schema(node.body, locals_map, call_types),
            infer_value_schema(node.orelse, locals_map, call_types),
        )
    # ``a or b`` / ``a and b`` — describe the operand we can.
    if isinstance(node, ast.BoolOp):
        schemas = [
            infer_value_schema(value, locals_map, call_types) for value in node.values
        ]
        chosen = next((schema for schema in schemas if schema != ANY_SCHEMA), ANY_SCHEMA)
        return dict(chosen)
    return dict(ANY_SCHEMA)


def _is_none_literal(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


def _nullable(schema: dict[str, Any]) -> dict[str, Any]:
    if schema == ANY_SCHEMA:
        return dict(ANY_SCHEMA)
    out = dict(schema)
    out["nullable"] = True
    return out


def unify(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    """Merge two candidate schemas for the same key (ternary branches)."""
    if a == b:
        return dict(a)
    if a == ANY_SCHEMA or b == ANY_SCHEMA:
        return dict(ANY_SCHEMA)
    if a.get("type") == b.get("type"):
        out = dict(a)
        if a.get("nullable") or b.get("nullable"):
            out["nullable"] = True
        return out
    return dict(ANY_SCHEMA)


def dict_payload(node: ast.AST) -> ast.Dict | None:
    """Return the payload ``Dict`` a ``return`` statement produces.

    Handles a bare ``return {...}`` and registered pass-through wrappers. The
    ``return self._sanitize(data)``
    variable case is resolved by the extractor, which tracks the variable.
    """
    if isinstance(node, ast.Dict):
        return node
    if isinstance(node, ast.Call) and node.args:
        name = _callee_name(node.func)
        if name in PASSTHROUGH_WRAPPERS and isinstance(node.args[0], ast.Dict):
            return node.args[0]
    return None


def passthrough_var(node: ast.AST) -> str | None:
    """Return the variable a ``return`` yields, through pass-through wrappers."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Call) and node.args:
        name = _callee_name(node.func)
        if name in PASSTHROUGH_WRAPPERS and isinstance(node.args[0], ast.Name):
            return node.args[0].id
    return None
