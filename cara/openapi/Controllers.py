"""Resolve which resource each controller action serializes, by AST.

Controllers keep public route methods thin and may place the implementation on
a local mixin or a private handler. Spec generation must inspect the same
MRO-visible method graph Python does; otherwise a legal file split silently
drops response contracts from the generated artifact.

Nothing here imports or boots the application: a generator that needed a
database, cache or broker could not run in a fast lane, and would couple the
published contract to runtime wiring.
"""

from __future__ import annotations

import ast
import re
from http import HTTPStatus
from pathlib import Path
from typing import Any

# ``types/Base.py`` imports nothing, so the no-boot promise in this module's
# docstring survives joining the taxonomy.

_ACTION_FUNCTIONS = tuple[ast.FunctionDef | ast.AsyncFunctionDef, ...]
_DECLARED_RESOURCE = re.compile(r"@resource\(\s*(\w+)\s*(\[\])?\s*\)")
_DECLARED_META = re.compile(r"@meta\(\s*(\w+)\s*\)")

# The response method that frames a payload as a page rather than a row. It is
# framework-owned, so it means the same thing in every application.
_PAGE_RESPONSE_METHOD = "paginated"
_RESPONSE_METHODS = frozenset(
    {
        "download",
        "envelope",
        "json",
        "no_content",
        "paginated",
        "redirect",
        "stream",
        "stream_csv",
        "stream_download",
        "stream_json_lines",
        "stream_sse",
        "to_response",
    }
)


def controller_action_functions(
    controllers_dir: Path,
) -> list[tuple[str, str, _ACTION_FUNCTIONS]]:
    """Resolve every controller action through its local delegates.

    Returns ``(controller, action, functions)`` triples where ``functions[0]``
    is the routed entrypoint and the rest are the same-class methods it calls,
    transitively. Base classes contribute their methods in Python's own
    resolution order, so a mixin split changes nothing about the result.
    """
    classes: dict[str, ast.ClassDef] = {}
    for path in sorted(controllers_dir.rglob("*.py")):
        if path.name == "__init__.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                classes[node.name] = node

    method_cache: dict[str, dict[str, Any]] = {}

    def visible_methods(class_name: str) -> dict[str, Any]:
        cached = method_cache.get(class_name)
        if cached is not None:
            return cached
        cls = classes[class_name]
        methods = {
            node.name: node
            for node in cls.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        # Python resolves the first base before later bases. Preserve that
        # order and let methods on the concrete class override every base.
        for base in cls.bases:
            if not isinstance(base, ast.Name) or base.id not in classes:
                continue
            for name, method in visible_methods(base.id).items():
                methods.setdefault(name, method)
        method_cache[class_name] = methods
        return methods

    actions: list[tuple[str, str, _ACTION_FUNCTIONS]] = []
    for controller in sorted(name for name in classes if name.endswith("Controller")):
        methods = visible_methods(controller)
        for action, entrypoint in methods.items():
            if action.startswith("_"):
                continue
            resolved: list[Any] = []
            pending = [entrypoint]
            visited: set[str] = set()
            while pending:
                method = pending.pop()
                if method.name in visited:
                    continue
                visited.add(method.name)
                resolved.append(method)
                for call in (
                    item for item in ast.walk(method) if isinstance(item, ast.Call)
                ):
                    func = call.func
                    if (
                        isinstance(func, ast.Attribute)
                        and isinstance(func.value, ast.Name)
                        and func.value.id == "self"
                        and func.attr in methods
                    ):
                        pending.append(methods[func.attr])
            actions.append((controller, action, tuple(resolved)))
    return actions


def cursor_paginated_actions(
    controllers_dir: Path,
    page_helpers: frozenset[str] | set[str] = frozenset(),
) -> set[str]:
    """Return controller actions that emit the canonical cursor envelope.

    ``response.paginated`` is the framework-owned wire signal and always
    counts. ``page_helpers`` names the application's own page-finishing
    functions — an action that builds its cursor through one of them pages by
    cursor even when it never touches the response helper directly. The
    framework does not guess at those names, because they are not its own.
    """
    helpers = frozenset(page_helpers)
    actions: set[str] = set()
    for controller, action, functions in controller_action_functions(controllers_dir):
        if any(
            any(
                isinstance(sub, ast.Call)
                and (
                    (isinstance(sub.func, ast.Name) and sub.func.id in helpers)
                    or (
                        isinstance(sub.func, ast.Attribute)
                        and sub.func.attr == _PAGE_RESPONSE_METHOD
                    )
                )
                for sub in ast.walk(fn)
            )
            for fn in functions
        ):
            actions.add(f"{controller}@{action}")
    return actions


def _request_name(call: ast.Call) -> str | None:
    """Recognize both supported FormRequest validation call styles."""
    func = call.func
    if not isinstance(func, ast.Attribute):
        return None
    if func.attr == "validate_request" and isinstance(func.value, ast.Call):
        constructor = func.value.func
        if isinstance(constructor, ast.Name) and constructor.id.endswith("Request"):
            return constructor.id
    if (
        func.attr == "validate"
        and isinstance(func.value, ast.Name)
        and func.value.id == "request"
        and call.args
        and isinstance(call.args[0], ast.Name)
        and call.args[0].id.endswith("Request")
    ):
        return call.args[0].id
    return None


def _status_values(node: ast.AST) -> set[int]:
    if isinstance(node, ast.Constant) and isinstance(node.value, int):
        return {node.value}
    if (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "HTTPStatus"
        and node.attr in HTTPStatus.__members__
    ):
        return {int(HTTPStatus[node.attr])}
    if isinstance(node, ast.IfExp):
        return _status_values(node.body) | _status_values(node.orelse)
    return set()


def _response_kind(method: str) -> str:
    if method == "no_content":
        return "empty"
    if method == "redirect":
        return "redirect"
    if method == "json":
        return "json"
    if method == "download" or method.startswith("stream"):
        return "binary"
    return "envelope"


def _response_statuses(call: ast.Call, method: str) -> set[int]:
    for keyword in call.keywords:
        if keyword.arg == "status":
            return _status_values(keyword.value) or {200}
    positional_index = {"json": 1, "redirect": 1, "envelope": 2}.get(method)
    if positional_index is not None and len(call.args) > positional_index:
        return _status_values(call.args[positional_index]) or {200}
    if method == "no_content":
        return {204}
    if method == "redirect":
        return {302}
    return {200}
