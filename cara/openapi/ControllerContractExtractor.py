"""Canonical definition of ``ControllerContractExtractor``."""

from __future__ import annotations

import ast
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .ControllerContract import ControllerContract
from .ControllerResponse import ControllerResponse
from .Controllers import (
    _RESOURCE_STATUS_METHOD,
    _RESPONSE_METHODS,
    _chained_with_status,
    _declared_raises,
    _request_name,
    _resource_statuses,
    _response_kind,
    _response_statuses,
    _status_bindings,
    controller_action_functions,
)
from .ReachableErrors import ReachableErrors
from .SourceClass import SourceClass
from .SourceIndex import SourceIndex
from .Sources import _import_root, _module_name
from .UnknownDeclaredError import UnknownDeclaredError


class ControllerContractExtractor:
    """Statically map routed actions to their input and response variants.

    ``error_statuses`` (exception class name -> HTTP status, from
    ``ErrorContractExtractor.exception_statuses``) documents the errors an
    action answers with: every typed exception that can escape it, followed
    through the services, repositories and helpers it calls
    (``ReachableErrors``), and every one its docstrings declare with
    ``@raises(...)`` for what flows through a call the source does not pin.
    Without it only the response calls are read.

    The controllers are read as modules of the package tree they sit in;
    ``module_aliases`` names the packages a deployable mirrors under another
    name at runtime (its kernel barrels: ``{"app.gates": "commons.gates"}``).
    ``routes`` (``parse_routes`` rows) give each action the HTTP method its
    request arrives with, so a raise only an unsafe method reaches is not
    listed on a read. An action routed under several methods lists what any
    of them can raise; an unrouted one is read with no method.
    """

    def __init__(
        self,
        controllers_dir: Path,
        error_statuses: Mapping[str, int] | None = None,
        *,
        module_aliases: Mapping[str, str] | None = None,
        routes: Iterable[Mapping[str, Any]] = (),
    ):
        self._dir = controllers_dir
        self._errors = dict(error_statuses or {})
        self._aliases = dict(module_aliases or {})
        self._methods: dict[str, set[str]] = {}
        for route in routes:
            action = f"{route['controller']}@{route['action']}"
            self._methods.setdefault(action, set()).add(str(route["method"]).upper())
        self._reached: tuple[dict[str, SourceClass], ReachableErrors | None] | None = None

    def extract(self) -> dict[str, ControllerContract]:
        contracts: dict[str, ControllerContract] = {}
        for controller, action, functions in controller_action_functions(self._dir):
            requests: set[str] = set()
            responses: set[ControllerResponse] = set()
            errors = {
                ControllerResponse(status, "error")
                for _, status in self._reachable(controller, action)
            }
            for function in functions:
                bindings = _status_bindings(function, functions)
                errors.update(self._declared(controller, action, function))
                for node in ast.walk(function):
                    if not isinstance(node, ast.Call):
                        continue
                    if request := _request_name(node):
                        requests.add(request)
                    func = node.func
                    if not isinstance(func, ast.Attribute):
                        continue
                    method = func.attr
                    if method == _RESOURCE_STATUS_METHOD:
                        responses.update(
                            ControllerResponse(status, "envelope")
                            for status in _resource_statuses(node, bindings)
                        )
                        continue
                    if method not in _RESPONSE_METHODS or _chained_with_status(func):
                        continue
                    kind = _response_kind(method)
                    responses.update(
                        ControllerResponse(status, kind)
                        for status in _response_statuses(node, method, bindings)
                    )
            if not responses:
                responses.add(ControllerResponse(200, "envelope"))
            # A status the action already answers in its own words keeps them.
            answered = {variant.status for variant in responses}
            responses.update(error for error in errors if error.status not in answered)
            contracts[f"{controller}@{action}"] = ControllerContract(
                requests=tuple(sorted(requests)),
                responses=tuple(sorted(responses)),
            )
        return contracts

    def redundant_declarations(self) -> list[str]:
        """``Controller.method: Name`` for every ``@raises`` declaration the
        call following already reaches in every action that runs it. Such a
        declaration is a second copy: it would keep publishing the status
        after the code below stopped raising it."""
        if self._reach()[1] is None:
            return []
        verdicts: dict[tuple[int, str], tuple[str, bool]] = {}
        for controller, action, functions in controller_action_functions(self._dir):
            reached = {name for name, _ in self._reachable(controller, action)}
            for function in functions:
                for name in _declared_raises(function):
                    label, redundant = verdicts.get(
                        (id(function), name),
                        (f"{controller}.{function.name}: {name}", True),
                    )
                    verdicts[(id(function), name)] = (
                        label,
                        redundant and name in reached,
                    )
        return sorted(label for label, redundant in verdicts.values() if redundant)

    def _reachable(self, controller: str, action: str) -> frozenset[tuple[str, int]]:
        """What can escape ``controller@action`` under any method it is
        routed with (``(exception name, status)``)."""
        classes, reach = self._reach()
        if reach is None or controller not in classes:
            return frozenset()
        methods = sorted(self._methods.get(f"{controller}@{action}", ()))
        return frozenset().union(
            *(
                reach.method_errors(classes[controller], action, http_method=method)
                for method in methods or [None]
            )
        )

    def _reach(self) -> tuple[dict[str, SourceClass], ReachableErrors | None]:
        """Every controller class as a module symbol, and the call follower
        that reads what escapes its actions (built once per extractor)."""
        if self._reached is not None:
            return self._reached
        if not self._errors:
            self._reached = ({}, None)
            return self._reached
        root = _import_root(self._dir)
        index = SourceIndex((root,), self._aliases)
        classes: dict[str, SourceClass] = {}
        for path in sorted(self._dir.rglob("*.py")):
            module = index.module(_module_name(root, path))
            if module is None:
                continue
            for name, node in module.classes.items():
                classes[name] = SourceClass(module, node)
        self._reached = (classes, ReachableErrors(index, self._errors))
        return self._reached

    def _declared(
        self,
        controller: str,
        action: str,
        function: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> set[ControllerResponse]:
        """The error variants ``function``'s docstring declares."""
        found: set[ControllerResponse] = set()
        for name in _declared_raises(function):
            status = self._errors.get(name)
            if status is None:
                raise UnknownDeclaredError(
                    f"{controller}@{action}: @raises({name}) names no exception "
                    "with a known status_code."
                )
            found.add(ControllerResponse(status, "error"))
        return found
