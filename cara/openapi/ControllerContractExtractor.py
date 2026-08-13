"""Canonical definition of ``ControllerContractExtractor``."""

from __future__ import annotations

import ast
from pathlib import Path

from .ControllerContract import ControllerContract
from .ControllerResponse import ControllerResponse
from .Controllers import (
    _RESPONSE_METHODS,
    _request_name,
    _response_kind,
    _response_statuses,
    controller_action_functions,
)


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
