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
from .UnknownDeclaredResource import UnknownDeclaredResource


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
        * a registered serialization helper call  -> its declared framing
        * ``Resource(x).to_array()/.to_dict()``   -> (Resource, single)
        * a bare ``Resource(x)`` construction

        ``response.paginated(...)`` forces list framing.
        """
        declared = self._declared_resource(functions[0])
        if declared is not None:
            return declared

        list_resource: str | None = None
        single_resource: str | None = None
        helper_resource: tuple[str, bool] | None = None
        paginated = False

        for fn in functions:
            for sub in ast.walk(fn):
                if not isinstance(sub, ast.Call):
                    continue
                func = sub.func
                # ``response.paginated(...)`` -> list envelope.
                if isinstance(func, ast.Attribute) and func.attr == _PAGE_RESPONSE_METHOD:
                    paginated = True
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
                    single_resource = func.id

        if list_resource is not None:
            return (list_resource, True)
        if helper_resource is not None:
            return helper_resource
        if single_resource is not None:
            return (single_resource, paginated)
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
