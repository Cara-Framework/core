"""Derive one OpenAPI component schema per resource class with AST.

Resource classes ARE the de-facto response shape of every endpoint, but
nothing introspects them, so a renamed key is caught only at runtime. This
extractor reads the serializer method of each class under a resources tree and
emits a FIELD-NAME-ACCURATE schema with best-effort types.

Honest limitations, by design:

* A key's type is derived only when its value expression is self-describing
  (see :mod:`cara.openapi.Inference`); anything else is emitted as ``{}``.
* When the payload continues one built elsewhere — ``payload =
  model.serialize()``, ``self.normalize(...)`` or a ``**spread`` this AST
  cannot read — the base keys are not statically knowable, so the schema is
  left OPEN (``additionalProperties: true`` plus the ``x-fields-partial``
  marker). Explicit keys added on top of such a base ARE captured. Publishing
  a closed schema for a payload we cannot fully see would swap "we cannot
  introspect this" for a confident wrong answer, which is worse than saying so.
* A payload that continues another RESOURCE's — ``super().to_dict()`` or
  ``SomeResource(x).to_dict()``, assigned or spread — takes that resource's
  schema under its own keys, and stays open only if that resource is.
* A subclass that defines no method at all — only class attributes, the
  switches a base serializer branches on — serializes through its base, so it
  takes the base's schema under its own name. A key such a switch can drop is
  set in a branch, which the base schema already leaves optional.
"""

from __future__ import annotations

import ast
import copy
from functools import reduce
from pathlib import Path
from typing import Any

from .Inference import (
    ANY_SCHEMA,
    CALL_TYPE,
    DYNAMIC_BASE_CALLS,
    SERIALIZER_METHODS,
    dict_payload,
    infer_value_schema,
    passthrough_var,
    unify,
)

# Modules in a resources tree that never define a serializable resource.
_SKIPPED_MODULES = ("__init__.py", "Serialization.py")


def _callee_name(func: ast.AST) -> str | None:
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def _base_names(cls: ast.ClassDef) -> list[str]:
    return [name for base in cls.bases if (name := _callee_name(base))]


def _opened(schema: dict[str, Any]) -> dict[str, Any]:
    """``schema`` declared to carry keys this extraction cannot name."""
    return {**schema, "additionalProperties": True, "x-fields-partial": True}


def _parent_payload(node: ast.AST, cls: ast.ClassDef) -> list[str] | None:
    """The resources a ``super().to_dict()`` / ``Resource(x).to_dict()`` call
    stands for, or None when ``node`` is neither."""
    if not (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in SERIALIZER_METHODS
        and isinstance(node.func.value, ast.Call)
        and isinstance(node.func.value.func, ast.Name)
    ):
        return None
    constructor = node.func.value.func.id
    if constructor == "super":
        # A bare ``object`` names no schema, so the caller leaves it open.
        return _base_names(cls) or ["object"]
    return [constructor] if constructor.endswith("Resource") else None


class ResourceSchemaExtractor:
    """AST-extract a field-name-accurate component schema per resource class.

    ``extra_call_types`` registers an application's own coercion helpers —
    ``name -> JSON-schema`` — alongside the framework's ``opt_*`` wrappers, so
    a house helper types its keys instead of degrading them to ``{}``.
    """

    def __init__(
        self,
        resources_dir: Path,
        extra_call_types: dict[str, dict[str, Any]] | None = None,
    ):
        self._dir = resources_dir
        self._call_types = {**CALL_TYPE, **(extra_call_types or {})}

    def extract(self) -> dict[str, dict[str, Any]]:
        schemas: dict[str, dict[str, Any]] = {}
        attribute_only: dict[str, list[str]] = {}
        continuing: dict[str, list[str]] = {}
        classes: dict[str, ast.ClassDef] = {}
        for path in sorted(self._dir.rglob("*.py")):
            if path.name in _SKIPPED_MODULES:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ClassDef):
                    continue
                parents: list[str] = []
                schema = self._class_schema(node, parents)
                if schema is not None:
                    schemas[node.name] = schema
                    classes[node.name] = node
                    if parents:
                        continuing[node.name] = parents
                elif node.bases and self._defines_no_method(node):
                    attribute_only[node.name] = _base_names(node)
        # Resolve subclass chains in any order: each pass settles every class
        # whose base is already final, until a pass settles none.
        while attribute_only or continuing:
            final = {
                name
                for name in schemas
                if name not in attribute_only and name not in continuing
            }
            copies = {
                name: base
                for name, bases in attribute_only.items()
                if (base := next((b for b in bases if b in final), None))
            }
            extended = {
                name: parent
                for name, parents in continuing.items()
                if (parent := next((p for p in parents if p in final), None))
            }
            if not copies and not extended:
                break
            for name, base in copies.items():
                schemas[name] = {**copy.deepcopy(schemas[base]), "x-resource": name}
                del attribute_only[name]
            for name, parent in extended.items():
                schemas[name] = self._over_parent(
                    schemas[name], schemas[parent], classes[name]
                )
                del continuing[name]
        # A parent nothing here can read: its keys exist and cannot be named.
        for name in continuing:
            schemas[name] = _opened(schemas[name])
        return schemas

    def _over_parent(
        self, schema: dict[str, Any], parent: dict[str, Any], cls: ast.ClassDef
    ) -> dict[str, Any]:
        """``schema``'s own keys on top of the resource its payload continues,
        less the ones ``cls`` takes off that payload."""
        if "properties" not in parent:
            # A ``oneOf`` parent: which variant it continues is a run-time fact.
            return _opened(schema)
        always, sometimes = self._removed_keys(self._serializer_method(cls))
        inherited = {
            key: item
            for key, item in copy.deepcopy(parent["properties"]).items()
            if key not in always
        }
        merged: dict[str, Any] = {
            **schema,
            "properties": {**inherited, **schema.get("properties", {})},
        }
        required = [
            *(key for key in parent.get("required", []) if key in inherited),
            *schema.get("required", []),
        ]
        required = [key for key in dict.fromkeys(required) if key not in sometimes]
        if required:
            merged["required"] = required
        return _opened(merged) if parent.get("x-fields-partial") else merged

    def _removed_keys(self, fn: ast.AST) -> tuple[set[str], set[str]]:
        """The keys ``fn`` deletes from the payload it returns: on every path,
        and on only some of them."""
        returned = {
            name
            for statement in self._returns(fn)
            if statement.value is not None
            if (name := passthrough_var(statement.value)) is not None
        }

        def removal(node: ast.AST) -> str | None:
            if isinstance(node, ast.Subscript):
                holder, key = node.value, node.slice
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "pop"
                and node.args
            ):
                holder, key = node.func.value, node.args[0]
            else:
                return None
            if (
                isinstance(holder, ast.Name)
                and holder.id in returned
                and isinstance(key, ast.Constant)
                and isinstance(key.value, str)
            ):
                return key.value
            return None

        always: set[str] = set()
        sometimes: set[str] = set()
        for node in ast.walk(fn):
            if isinstance(node, ast.Delete):
                targets = node.targets
            elif isinstance(node, ast.Call):
                targets = [node]
            else:
                continue
            removed = {key for target in targets if (key := removal(target)) is not None}
            (always if self._is_unconditional(fn, node) else sometimes).update(removed)
        return always, sometimes

    @staticmethod
    def _defines_no_method(cls: ast.ClassDef) -> bool:
        return not any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) for node in cls.body
        )

    def _serializer_method(self, cls: ast.ClassDef) -> ast.AST | None:
        for fn in cls.body:
            if (
                isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef))
                and fn.name in SERIALIZER_METHODS
            ):
                return fn
        return None

    def _class_schema(
        self, cls: ast.ClassDef, parents: list[str]
    ) -> dict[str, Any] | None:
        """``cls``'s own schema, or None for a class with no serializer.
        ``parents`` receives the resources its payload continues, which the
        caller folds in once every class has a schema."""
        fn = self._serializer_method(cls)
        if fn is None:
            return None

        properties: dict[str, Any] = {}
        required: list[str] = []
        # Tracks the local variable holding the payload dict that gets returned
        # (e.g. ``data = model.serialize()`` then ``data["x"] = ...``). If the
        # base came from elsewhere we cannot know its keys -> open schema.
        open_schema = False

        # Pre-scan simple typed local bindings (``amount = self.opt_float(...)``)
        # so the precompute-then-reference pattern recovers the key's type.
        locals_map = self._local_bindings(fn)

        returns = self._returns(fn)
        direct_payloads = [
            payload
            for statement in returns
            if statement.value is not None
            if (payload := dict_payload(statement.value)) is not None
        ]
        if len(direct_payloads) > 1 and len(direct_payloads) == len(returns):
            variants = self._distinct_variants(direct_payloads, locals_map)
            if len(variants) > 1:
                return {"oneOf": variants, "x-resource": cls.name}

        # 1) Returned dict literal(s), including pass-through wrappers.
        returned_var: str | None = None
        for sub in returns:
            if sub.value is not None:
                payload = dict_payload(sub.value)
                if payload is not None:
                    self._merge_dict_keys(payload, properties, locals_map)
                    self._append_literal_keys(payload, required)
                    open_schema |= self._merge_spreads(
                        payload, cls, properties, locals_map, parents
                    )
                # ``return self._sanitize(data)`` / ``return data``.
                var = passthrough_var(sub.value)
                if var is not None:
                    returned_var = var

        # 2) The literal that BUILT the returned variable: ``payload = {...}``.
        # Without this, a resource that returns a variable contributes nothing
        # in step 1, and a single later ``payload["k"] = v`` would be enough to
        # make ``properties`` non-empty — suppressing the open-schema fallback
        # below and publishing a schema claiming the resource had exactly that
        # ONE key.
        if returned_var is not None:
            for sub in ast.walk(fn):
                if (
                    isinstance(sub, ast.Assign)
                    and len(sub.targets) == 1
                    and isinstance(sub.targets[0], ast.Name)
                    and sub.targets[0].id == returned_var
                ):
                    literal = dict_payload(sub.value)
                    if literal is not None:
                        self._merge_dict_keys(literal, properties, locals_map)
                        self._append_literal_keys(literal, required)
                        open_schema |= self._merge_spreads(
                            literal, cls, properties, locals_map, parents
                        )
                    elif (names := _parent_payload(sub.value, cls)) is not None:
                        parents.extend(names)

        # 3) Subscript assignments and ``.update({...})`` onto the returned
        # variable — conditionally attached keys (gated blocks, optional
        # columns).
        if returned_var is not None:
            for sub in ast.walk(fn):
                if (
                    isinstance(sub, ast.Assign)
                    and len(sub.targets) == 1
                    and isinstance(sub.targets[0], ast.Subscript)
                    and isinstance(sub.targets[0].value, ast.Name)
                    and sub.targets[0].value.id == returned_var
                ):
                    key_node = sub.targets[0].slice
                    if isinstance(key_node, ast.Constant) and isinstance(
                        key_node.value, str
                    ):
                        properties[key_node.value] = infer_value_schema(
                            sub.value, locals_map, self._call_types
                        )
                        if self._is_unconditional(fn, sub):
                            self._append_required(required, key_node.value)
                    else:
                        # The key is computed at run time (a loop variable, a
                        # lookup): the payload carries keys this cannot name.
                        open_schema = True
                elif (
                    isinstance(sub, ast.Call)
                    and isinstance(sub.func, ast.Attribute)
                    and sub.func.attr == "update"
                    and isinstance(sub.func.value, ast.Name)
                    and sub.func.value.id == returned_var
                ):
                    if sub.args and isinstance(sub.args[0], ast.Dict):
                        self._merge_dict_keys(sub.args[0], properties, locals_map)
                        if self._is_unconditional(fn, sub):
                            self._append_literal_keys(sub.args[0], required)
                    else:
                        # Merged from a value we cannot read -> unknown keys.
                        open_schema = True
            # Does that variable continue a payload built elsewhere? Then the
            # base keys are dynamic -> open schema.
            if self._var_from_dynamic_base(fn, returned_var):
                open_schema = True

        if not properties and not open_schema and not parents:
            # Nothing introspectable (pure ``return self.resource.serialize()``).
            open_schema = True

        schema: dict[str, Any] = {"type": "object", "properties": properties}
        if required:
            schema["required"] = required
        if open_schema:
            schema["additionalProperties"] = True
            schema["x-fields-partial"] = True
        schema["x-resource"] = cls.name
        return schema

    def _dict_variant(
        self,
        payload: ast.Dict,
        locals_map: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        properties: dict[str, Any] = {}
        required: list[str] = []
        self._merge_dict_keys(payload, properties, locals_map)
        self._append_literal_keys(payload, required)
        schema: dict[str, Any] = {
            "type": "object",
            "properties": properties,
        }
        if required:
            schema["required"] = required
        if len(required) != len(payload.keys):
            schema["additionalProperties"] = True
            schema["x-fields-partial"] = True
        return schema

    def _distinct_variants(
        self,
        payloads: list[ast.Dict],
        locals_map: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        variants: list[dict[str, Any]] = []
        for payload in payloads:
            variant = self._dict_variant(payload, locals_map)
            if variant not in variants:
                variants.append(variant)
        return variants

    @staticmethod
    def _append_required(required: list[str], key: str) -> None:
        if key not in required:
            required.append(key)

    def _append_literal_keys(self, payload: ast.Dict, required: list[str]) -> None:
        for key in payload.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                self._append_required(required, key.value)

    @staticmethod
    def _returns(fn: ast.AST) -> list[ast.Return]:
        """Return statements owned by this serializer, excluding nested defs."""

        class Visitor(ast.NodeVisitor):
            def __init__(self, root: ast.AST):
                self.root = root
                self.items: list[ast.Return] = []

            def visit_Return(self, node: ast.Return) -> None:  # noqa: N802
                self.items.append(node)

            def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
                if node is self.root:
                    self.generic_visit(node)

            def visit_AsyncFunctionDef(  # noqa: N802
                self,
                node: ast.AsyncFunctionDef,
            ) -> None:
                if node is self.root:
                    self.generic_visit(node)

            def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802
                return

            def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
                return

        visitor = Visitor(fn)
        visitor.visit(fn)
        return visitor.items

    @staticmethod
    def _is_unconditional(fn: ast.AST, target: ast.AST) -> bool:
        """Whether a mutation is outside conditional/control-flow blocks."""

        parents: dict[ast.AST, ast.AST] = {}
        for parent in ast.walk(fn):
            for child in ast.iter_child_nodes(parent):
                parents[child] = parent
        conditional = (ast.If, ast.For, ast.AsyncFor, ast.While, ast.Try, ast.Match)
        try_star = getattr(ast, "TryStar", ast.Try)
        node = target
        while node in parents and parents[node] is not fn:
            node = parents[node]
            if isinstance(node, (*conditional, try_star)):
                return False
        return True

    def _local_bindings(self, fn: ast.AST) -> dict[str, dict[str, Any]]:
        """Map ``name -> schema`` for simple single-target local assignments
        whose value is itself type-derivable (an ``opt_*`` wrapper / cast /
        literal). Used to resolve bare ``Name`` values in the payload dict.

        A name assigned twice can hold either value: its schema is the union,
        and one assignment we cannot read leaves it unknown. (``status =
        row.status`` then ``status = "expired"`` used to document only
        ``expired``.)"""
        assigned: dict[str, list[dict[str, Any]]] = {}
        for sub in ast.walk(fn):
            if (
                isinstance(sub, ast.Assign)
                and len(sub.targets) == 1
                and isinstance(sub.targets[0], ast.Name)
            ):
                assigned.setdefault(sub.targets[0].id, []).append(
                    infer_value_schema(sub.value, None, self._call_types)
                )
        return {
            name: reduce(unify, schemas)
            for name, schemas in assigned.items()
            if ANY_SCHEMA not in schemas
        }

    def _merge_dict_keys(
        self,
        payload: ast.Dict,
        properties: dict[str, Any],
        locals_map: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        for key, value in zip(payload.keys, payload.values, strict=False):
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                inferred = infer_value_schema(value, locals_map, self._call_types)
                if key.value in properties:
                    properties[key.value] = unify(properties[key.value], inferred)
                else:
                    properties[key.value] = inferred

    def _merge_spreads(
        self,
        payload: ast.Dict,
        cls: ast.ClassDef,
        properties: dict[str, Any],
        locals_map: dict[str, dict[str, Any]],
        parents: list[str],
    ) -> bool:
        """Fold ``payload``'s ``**spread`` entries in; True when one carries
        keys this cannot name. A spread key is optional: helpers answer ``{}``
        when their data was not attached."""
        unreadable = False
        for key, value in zip(payload.keys, payload.values, strict=False):
            if key is not None:
                continue
            if (names := _parent_payload(value, cls)) is not None:
                parents.extend(names)
            elif not self._merge_spread(value, cls, properties, locals_map):
                unreadable = True
        return unreadable

    def _merge_spread(
        self,
        node: ast.AST,
        cls: ast.ClassDef,
        properties: dict[str, Any],
        locals_map: dict[str, dict[str, Any]],
    ) -> bool:
        """Merge the keys a spread expression yields; False if it cannot tell."""
        if isinstance(node, ast.Dict):
            self._merge_dict_keys(node, properties, locals_map)
            return all(
                self._merge_spread(value, cls, properties, locals_map)
                for key, value in zip(node.keys, node.values, strict=False)
                if key is None
            )
        if isinstance(node, ast.IfExp):
            body = self._merge_spread(node.body, cls, properties, locals_map)
            orelse = self._merge_spread(node.orelse, cls, properties, locals_map)
            return body and orelse
        # ``**self._helper(row)``: a method of this class that returns dicts.
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "self"
        ):
            return False
        helper = next(
            (
                item
                for item in cls.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                and item.name == node.func.attr
            ),
            None,
        )
        if helper is None:
            return False
        returns = self._returns(helper)
        helper_locals = self._local_bindings(helper)
        return bool(returns) and all(
            statement.value is not None
            and self._merge_spread(statement.value, cls, properties, helper_locals)
            for statement in returns
        )

    @staticmethod
    def _var_from_dynamic_base(fn: ast.AST, var: str) -> bool:
        """True if ``var`` starts from a model/normaliser call this method did
        not build (``var = row.serialize()`` / ``self.normalize(...)``)."""
        for sub in ast.walk(fn):
            if (
                isinstance(sub, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == var for t in sub.targets)
                and isinstance(sub.value, ast.Call)
                and _callee_name(sub.value.func) in DYNAMIC_BASE_CALLS
            ):
                return True
        return False
