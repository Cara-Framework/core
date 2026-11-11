"""Canonical definition of ``ControllerContractExtractor``."""

from __future__ import annotations

import ast
from collections.abc import Mapping
from pathlib import Path

from .ControllerContract import ControllerContract
from .ControllerResponse import ControllerResponse
from .Controllers import (
    _RESOURCE_STATUS_METHOD,
    _RESPONSE_METHODS,
    _chained_with_status,
    _declared_raises,
    _raised_names,
    _request_name,
    _resource_statuses,
    _response_kind,
    _response_statuses,
    _status_bindings,
    controller_action_functions,
)
from .UnknownDeclaredError import UnknownDeclaredError


class ControllerContractExtractor:
    """Statically map routed actions to their input and response variants.

    ``error_statuses`` (exception class name -> HTTP status, from
    ``ErrorContractExtractor.exception_statuses``) documents the errors an
    action answers with: every typed exception its own method graph raises,
    and every one its docstrings declare with ``@raises(...)`` for what a
    service raises below it. Without it only the response calls are read.
    """

    def __init__(
        self,
        controllers_dir: Path,
        error_statuses: Mapping[str, int] | None = None,
    ):
        self._dir = controllers_dir
        self._errors = dict(error_statuses or {})

    def extract(self) -> dict[str, ControllerContract]:
        contracts: dict[str, ControllerContract] = {}
        for controller, action, functions in controller_action_functions(self._dir):
            requests: set[str] = set()
            responses: set[ControllerResponse] = set()
            errors: set[ControllerResponse] = set()
            for function in functions:
                bindings = _status_bindings(function, functions)
                errors.update(self._raised(controller, action, function))
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

    def _raised(
        self,
        controller: str,
        action: str,
        function: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> set[ControllerResponse]:
        """The error variants ``function`` raises or declares."""
        found: set[ControllerResponse] = set()
        for name in _raised_names(function):
            status = self._errors.get(name)
            if status is not None and status >= 400:
                found.add(ControllerResponse(status, "error"))
        for name in _declared_raises(function):
            status = self._errors.get(name)
            if status is None:
                raise UnknownDeclaredError(
                    f"{controller}@{action}: @raises({name}) names no exception "
                    "with a known status_code."
                )
            found.add(ControllerResponse(status, "error"))
        return found
