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
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Any

# ``types/Base.py`` imports nothing, so the no-boot promise in this module's
# docstring survives joining the taxonomy.
from cara.exceptions.types.Base import CaraException

_ACTION_FUNCTIONS = tuple[ast.FunctionDef | ast.AsyncFunctionDef, ...]
_DECLARED_RESOURCE = re.compile(r"@resource\(\s*(\w+)\s*(\[\])?\s*\)")

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


@dataclass(frozen=True, slots=True, order=True)
class ControllerResponse:
    """One statically observed response variant for a controller action."""

    status: int
    kind: str


@dataclass(frozen=True, slots=True)
class ControllerContract:
    """Request validators and response variants used by one routed action."""

    requests: tuple[str, ...]
    responses: tuple[ControllerResponse, ...]


class UnknownDeclaredResource(CaraException, RuntimeError):
    """An ``@resource(...)`` docstring names a resource that does not exist.

    In the taxonomy (§9) so ``except CaraException`` around spec generation
    catches it; ``RuntimeError`` stays as a SECOND base for the craft command
    that already treats a RuntimeError as "fail this build, print the message".
    """


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


class ControllerContractExtractor:
    """Statically map routed actions to their input and response variants."""

    def __init__(self, controllers_dir: Path):
        self._dir = controllers_dir

    def extract(self) -> dict[str, ControllerContract]:
        contracts: dict[str, ControllerContract] = {}
        for controller, action, functions in controller_action_functions(self._dir):
            requests: set[str] = set()
            responses: set[ControllerResponse] = set()
            for function in functions:
                for node in ast.walk(function):
                    if not isinstance(node, ast.Call):
                        continue
                    if request := _request_name(node):
                        requests.add(request)
                    func = node.func
                    if not isinstance(func, ast.Attribute):
                        continue
                    method = func.attr
                    if method not in _RESPONSE_METHODS:
                        continue
                    kind = _response_kind(method)
                    responses.update(
                        ControllerResponse(status, kind)
                        for status in _response_statuses(node, method)
                    )
            if not responses:
                responses.add(ControllerResponse(200, "envelope"))
            contracts[f"{controller}@{action}"] = ControllerContract(
                requests=tuple(sorted(requests)),
                responses=tuple(sorted(responses)),
            )
        return contracts
