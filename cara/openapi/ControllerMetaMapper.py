"""Canonical definition of ``ControllerMetaMapper``."""

from __future__ import annotations

import ast
from pathlib import Path

from .Controllers import _DECLARED_META, controller_action_functions
from .UnknownDeclaredResource import UnknownDeclaredResource


class ControllerMetaMapper:
    """Map ``@meta(Resource)`` action declarations to resource schemas."""

    def __init__(self, controllers_dir: Path, known_resources: set[str]):
        self._dir = controllers_dir
        self._known = known_resources

    def map(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for controller, action, functions in controller_action_functions(self._dir):
            doc = ast.get_docstring(functions[0])
            match = _DECLARED_META.search(doc) if doc else None
            if match is None:
                continue
            name = match.group(1)
            if name not in self._known:
                raise UnknownDeclaredResource(
                    f"@meta({name}) declares an unknown resource. "
                    f"Known: {', '.join(sorted(self._known))}"
                )
            mapping[f"{controller}@{action}"] = name
        return mapping
