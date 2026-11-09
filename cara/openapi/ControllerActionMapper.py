"""Canonical definition of ``ControllerActionMapper``."""

from __future__ import annotations

import ast
from pathlib import Path

from .Controllers import (
    _ACTION_FUNCTIONS,
    _DECLARED_RESOURCE,
    _PAGE_RESPONSE_METHOD,
    controller_action_functions,
)
from .Inference import SERIALIZER_METHODS
from .UnknownDeclaredResource import UnknownDeclaredResource

# The response methods whose first argument is the envelope's ``data``.
_DATA_RESPONSE_METHODS = frozenset({"envelope", _PAGE_RESPONSE_METHOD})


class ControllerActionMapper:
    """AST-map ``Controller@action`` -> ``(resource_name, is_list)``.

    ``serializer_helpers`` lets an application register its own module-level
    serialization helpers — ``name -> (resource_name, is_list)`` — for actions
    that hand a collection to a helper instead of naming the resource.
    """

    def __init__(
        self,
        controllers_dir: Path,
        known_resources: set[str],
        serializer_helpers: dict[str, tuple[str, bool]] | None = None,
    ):
        self._dir = controllers_dir
        self._known = known_resources
        self._helpers = dict(serializer_helpers or {})

    def map(self) -> dict[str, tuple[str, bool]]:
        mapping: dict[str, tuple[str, bool]] = {}
        for controller, action, functions in controller_action_functions(self._dir):
            resolved = self._action_resource(functions)
            if resolved is not None:
                mapping[f"{controller}@{action}"] = resolved
        return mapping

    def _action_resource(self, functions: _ACTION_FUNCTIONS) -> tuple[str, bool] | None:
        """Find the resource an action serializes, and whether it is a list.

        Signals, strongest first:

        * ``@resource(Name)`` / ``@resource(Name[])`` in the action docstring
        * ``Resource.collection(...)``            -> (Resource, list)
        * ``[Resource(x).to_dict() for x in rows]`` as the response's data
                                                  -> (Resource, list)
        * a registered serialization helper call  -> its declared framing
        * ``Resource(x).to_array()/.to_dict()``   -> (Resource, single)
        * a bare ``Resource(x)`` construction

        ``response.paginated(...)`` forces list framing. A comprehension's rows
        that are only a PART of the data (``{"sent": [...], "refused": n}``)
        say nothing about the data's own shape, so they do not map the action.
        """
        declared = self._declared_resource(functions[0])
        if declared is not None:
            return declared

        list_resource: str | None = None
        single_resource: str | None = None
        row_resource: str | None = None
        helper_resource: tuple[str, bool] | None = None
        paginated = False

        for fn in functions:
            rows = {id(call) for call in self._comprehension_rows(fn)}
            for sub in ast.walk(fn):
                if not isinstance(sub, ast.Call):
                    continue
                func = sub.func
                # ``response.paginated(...)`` -> list envelope.
                if isinstance(func, ast.Attribute) and func.attr == _PAGE_RESPONSE_METHOD:
                    paginated = True
                # ``response.envelope([Resource(r).to_dict() for r in rows])``.
                if (
                    isinstance(func, ast.Attribute)
                    and func.attr in _DATA_RESPONSE_METHODS
                    and (rendered := self._data_rows_resource(sub, fn)) is not None
                ):
                    list_resource = rendered
                # A registered application serialization helper.
                if isinstance(func, ast.Name) and func.id in self._helpers:
                    candidate = self._helpers[func.id]
                    if candidate[0] in self._known:
                        helper_resource = candidate
                # ``Resource.collection(...)``
                if (
                    isinstance(func, ast.Attribute)
                    and func.attr == "collection"
                    and isinstance(func.value, ast.Name)
                    and func.value.id in self._known
                ):
                    list_resource = func.value.id
                # ``Resource(...)`` construction (single, unless paginated).
                if isinstance(func, ast.Name) and func.id in self._known:
                    if id(sub) in rows:
                        row_resource = func.id
                    else:
                        single_resource = func.id

        if list_resource is not None:
            return (list_resource, True)
        if helper_resource is not None:
            return helper_resource
        if single_resource is not None:
            return (single_resource, paginated)
        if row_resource is not None and paginated:
            return (row_resource, True)
        return None

    def _comprehension_rows(self, fn: ast.AST) -> list[ast.Call]:
        """Every ``Resource(...)`` a list comprehension builds once per row."""
        return [
            call
            for node in ast.walk(fn)
            if isinstance(node, ast.ListComp)
            for call in ast.walk(node.elt)
            if isinstance(call, ast.Call)
            and isinstance(call.func, ast.Name)
            and call.func.id in self._known
        ]

    def _data_rows_resource(self, call: ast.Call, fn: ast.AST) -> str | None:
        """The row resource when a response's data IS a list comprehension of
        one, written in place or through a local (``rows = [...]``)."""
        data = call.args[0] if call.args else None
        if isinstance(data, ast.Name):
            data = next(
                (
                    node.value
                    for node in ast.walk(fn)
                    if isinstance(node, ast.Assign)
                    and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name)
                    and node.targets[0].id == data.id
                ),
                None,
            )
        if not isinstance(data, ast.ListComp):
            return None
        element = data.elt
        if (
            isinstance(element, ast.Call)
            and isinstance(element.func, ast.Attribute)
            and element.func.attr in SERIALIZER_METHODS
        ):
            element = element.func.value
        if (
            isinstance(element, ast.Call)
            and isinstance(element.func, ast.Name)
            and element.func.id in self._known
        ):
            return element.func.id
        return None

    def _declared_resource(self, fn: ast.AST) -> tuple[str, bool] | None:
        """Read an explicit ``@resource(...)`` declaration from the docstring.

        The AST signals below can only see a resource the ACTION ITSELF names.
        When serialization legitimately lives below the controller — a service
        that serializes inside its own cache closure, so the cache stores a
        codec-safe dict rather than ORM rows — the action never mentions the
        resource and the route would silently lose its response schema. The
        docstring declaration is the explicit source of truth for that case,
        mirroring how the routing DSL already makes the controller docstring
        the route source of truth.

            @resource(SomeDetailResource)    -> single
            @resource(SomeRowResource[])     -> list
        """
        doc = ast.get_docstring(fn)
        if not doc:
            return None
        match = _DECLARED_RESOURCE.search(doc)
        if match is None:
            return None
        name, is_list = match.group(1), match.group(2) is not None
        # A declared-but-unknown resource is a typo that would emit a dangling
        # ``$ref`` (or silently drop the schema) — exactly the drift this
        # generator exists to catch, so fail loudly instead of degrading.
        if name not in self._known:
            raise UnknownDeclaredResource(
                f"@resource({name}) declares an unknown resource. "
                f"Known: {', '.join(sorted(self._known))}"
            )
        return (name, is_list)
