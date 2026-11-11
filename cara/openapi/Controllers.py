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
# ``@raises(InvalidInput, ConflictException)``: the typed exceptions an action
# answers with that are raised BELOW the controller (a service), where the
# action's own source cannot show them.
_DECLARED_RAISES = re.compile(r"@raises\(([^)]*)\)")

# The response method that frames a payload as a page rather than a row. It is
# framework-owned, so it means the same thing in every application.
_PAGE_RESPONSE_METHOD = "paginated"
# The resource-layer way to name a success status: ``.with_status(201)``, ahead
# of the ``.to_response(...)`` that then sends it.
_RESOURCE_STATUS_METHOD = "with_status"
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


def null_data_actions(controllers_dir: Path) -> set[str]:
    """Return controller actions that can answer ``data: null``, the ones that
    hand ``response.envelope`` a literal ``None`` on some path."""
    actions: set[str] = set()
    for controller, action, functions in controller_action_functions(controllers_dir):
        if any(
            isinstance(sub, ast.Call)
            and isinstance(sub.func, ast.Attribute)
            and sub.func.attr == "envelope"
            and sub.args
            and isinstance(sub.args[0], ast.Constant)
            and sub.args[0].value is None
            for fn in functions
            for sub in ast.walk(fn)
        ):
            actions.add(f"{controller}@{action}")
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


def _status_values(
    node: ast.AST, bindings: dict[str, set[int]] | None = None
) -> set[int]:
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
        return _status_values(node.body, bindings) | _status_values(node.orelse, bindings)
    if isinstance(node, ast.Name) and bindings:
        return set(bindings.get(node.id, ()))
    return set()


def _status_bindings(
    function: ast.FunctionDef | ast.AsyncFunctionDef, functions: _ACTION_FUNCTIONS
) -> dict[str, set[int]]:
    """The statuses each name in ``function`` can hold: its parameter defaults,
    what the action's own calls pass it, and its local assignments. A status
    is often one argument of a shared ``_respond(..., status=201)`` helper."""
    bindings: dict[str, set[int]] = {}

    def bind(name: str, value: ast.AST | None) -> None:
        if value is not None and (found := _status_values(value)):
            bindings.setdefault(name, set()).update(found)

    arguments = function.args
    positional = [*arguments.posonlyargs, *arguments.args]
    defaulted = positional[len(positional) - len(arguments.defaults) :]
    for parameter, default in zip(defaulted, arguments.defaults, strict=True):
        bind(parameter.arg, default)
    for parameter, default in zip(
        arguments.kwonlyargs, arguments.kw_defaults, strict=True
    ):
        bind(parameter.arg, default)

    for caller in functions:
        for call in (item for item in ast.walk(caller) if isinstance(item, ast.Call)):
            target = call.func
            if not (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "self"
                and target.attr == function.name
            ):
                continue
            for keyword in call.keywords:
                if keyword.arg is not None:
                    bind(keyword.arg, keyword.value)
            for parameter, value in zip(positional[1:], call.args, strict=False):
                bind(parameter.arg, value)

    for node in ast.walk(function):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            bind(node.targets[0].id, node.value)
    return bindings


def _raised_names(function: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    """The exception classes ``function`` raises by name: ``raise Name(...)``,
    ``raise Name`` or ``raise module.Name(...)``. A re-raised variable
    (``raise exc``) names no class an application maps, so it adds nothing."""
    names: set[str] = set()
    for node in ast.walk(function):
        if not isinstance(node, ast.Raise) or node.exc is None:
            continue
        raised = node.exc.func if isinstance(node.exc, ast.Call) else node.exc
        if isinstance(raised, ast.Name):
            names.add(raised.id)
        elif isinstance(raised, ast.Attribute):
            names.add(raised.attr)
    return names


def _declared_raises(function: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    """The exception classes ``function``'s docstring declares with ``@raises``."""
    doc = ast.get_docstring(function) or ""
    return [
        name.strip()
        for match in _DECLARED_RAISES.finditer(doc)
        for name in match.group(1).split(",")
        if name.strip()
    ]


def _chained_with_status(func: ast.Attribute) -> bool:
    """Whether a ``.to_response`` call is chained straight onto ``.with_status``:
    the status that call names is then the one this response answers."""
    receiver = func.value
    while isinstance(receiver, ast.Call) and isinstance(receiver.func, ast.Attribute):
        if receiver.func.attr == _RESOURCE_STATUS_METHOD:
            return func.attr == "to_response"
        receiver = receiver.func.value
    return False


def _resource_statuses(call: ast.Call, bindings: dict[str, set[int]]) -> set[int]:
    """The statuses a ``resource.with_status(...)`` call can set."""
    return _status_values(call.args[0], bindings) or {200} if call.args else {200}


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


def _response_statuses(
    call: ast.Call, method: str, bindings: dict[str, set[int]] | None = None
) -> set[int]:
    for keyword in call.keywords:
        if keyword.arg == "status":
            return _status_values(keyword.value, bindings) or {200}
    positional_index = {"json": 1, "redirect": 1, "envelope": 2}.get(method)
    if positional_index is not None and len(call.args) > positional_index:
        return _status_values(call.args[positional_index], bindings) or {200}
    if method == "no_content":
        return {204}
    if method == "redirect":
        return {302}
    return {200}
