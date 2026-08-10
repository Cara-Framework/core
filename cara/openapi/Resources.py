"""Derive one OpenAPI component schema per resource class, by AST.

Resource classes ARE the de-facto response shape of every endpoint, but
nothing introspects them, so a renamed key is caught only at runtime. This
extractor reads the serializer method of each class under a resources tree and
emits a FIELD-NAME-ACCURATE schema with best-effort types.

Honest limitations, by design:

* A key's type is derived only when its value expression is self-describing
  (see :mod:`cara.openapi.Inference`); anything else is emitted as ``{}``.
* When the payload continues one built elsewhere — ``payload =
  model.serialize()``, ``self.normalize(...)`` or ``super().to_dict()`` — the
  base keys are not statically knowable, so the schema is left OPEN
  (``additionalProperties: true`` plus the ``x-fields-partial`` marker).
  Explicit keys added on top of such a base ARE captured. Publishing a closed
  schema for a payload we cannot fully see would swap "we cannot introspect
  this" for a confident wrong answer, which is worse than saying so.
"""

from __future__ import annotations

import ast
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
        for path in sorted(self._dir.rglob("*.py")):
            if path.name in _SKIPPED_MODULES:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ClassDef):
                    continue
                schema = self._class_schema(node)
                if schema is not None:
                    schemas[node.name] = schema
        return schemas

    def _serializer_method(self, cls: ast.ClassDef) -> ast.AST | None:
        for fn in cls.body:
            if (
                isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef))
                and fn.name in SERIALIZER_METHODS
            ):
                return fn
        return None

    def _class_schema(self, cls: ast.ClassDef) -> dict[str, Any] | None:
        fn = self._serializer_method(cls)
        if fn is None:
            return None

        properties: dict[str, Any] = {}
        # Tracks the local variable holding the payload dict that gets returned
        # (e.g. ``data = model.serialize()`` then ``data["x"] = ...``). If the
        # base came from elsewhere we cannot know its keys -> open schema.
        open_schema = False

        # Pre-scan simple typed local bindings (``amount = self.opt_float(...)``)
        # so the precompute-then-reference pattern recovers the key's type.
        locals_map = self._local_bindings(fn)

        # 1) Returned dict literal(s), including pass-through wrappers.
        returned_var: str | None = None
        for sub in ast.walk(fn):
            if isinstance(sub, ast.Return) and sub.value is not None:
                payload = dict_payload(sub.value)
                if payload is not None:
                    self._merge_dict_keys(payload, properties, locals_map)
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
                    else:
                        # Merged from a value we cannot read -> unknown keys.
                        open_schema = True
            # Does that variable continue a payload built elsewhere? Then the
            # base keys are dynamic -> open schema.
            if self._var_from_dynamic_base(fn, returned_var):
                open_schema = True

        if not properties and not open_schema:
            # Nothing introspectable (pure ``return self.resource.serialize()``).
            open_schema = True

        schema: dict[str, Any] = {"type": "object", "properties": properties}
        if open_schema:
            schema["additionalProperties"] = True
            schema["x-fields-partial"] = True
        schema["x-resource"] = cls.name
        return schema

    def _local_bindings(self, fn: ast.AST) -> dict[str, dict[str, Any]]:
        """Map ``name -> schema`` for simple single-target local assignments
        whose value is itself type-derivable (an ``opt_*`` wrapper / cast /
        literal). Used to resolve bare ``Name`` values in the payload dict."""
        out: dict[str, dict[str, Any]] = {}
        for sub in ast.walk(fn):
            if (
                isinstance(sub, ast.Assign)
                and len(sub.targets) == 1
                and isinstance(sub.targets[0], ast.Name)
            ):
                schema = infer_value_schema(sub.value, None, self._call_types)
                if schema != ANY_SCHEMA:
                    out[sub.targets[0].id] = schema
        return out

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

    @staticmethod
    def _var_from_dynamic_base(fn: ast.AST, var: str) -> bool:
        """True if ``var`` starts from a payload this method did not build.

        Two shapes qualify: a model/normaliser call
        (``var = row.serialize()`` / ``self.normalize(...)``) and a parent
        serializer (``var = super().to_dict()``). A subclass that extends its
        parent's payload inherits every key the parent emits, so treating the
        subclass schema as closed would drop them all silently.
        """
        for sub in ast.walk(fn):
            if not (
                isinstance(sub, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == var for t in sub.targets)
                and isinstance(sub.value, ast.Call)
            ):
                continue
            func = sub.value.func
            name = _callee_name(func)
            if name in DYNAMIC_BASE_CALLS:
                return True
            if (
                name in SERIALIZER_METHODS
                and isinstance(func, ast.Attribute)
                and isinstance(func.value, ast.Call)
                and isinstance(func.value.func, ast.Name)
                and func.value.func.id == "super"
            ):
                return True
        return False
