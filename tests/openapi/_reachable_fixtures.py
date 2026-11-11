"""Shared package-tree fixtures for the ``ReachableErrors`` tests."""

from __future__ import annotations

from pathlib import Path

from cara.openapi import ControllerContractExtractor, ControllerResponse

STATUSES = {
    "InvalidInput": 400,
    "EntityNotFound": 404,
    "ConflictException": 409,
    "Refused": 409,
    "HttpException": 500,
    "ServiceException": 500,
}

EXCEPTIONS = """
class ServiceException(Exception):
    status_code = 500

class InvalidInput(ServiceException):
    status_code = 400

class EntityNotFound(ServiceException):
    status_code = 404

class ConflictException(ServiceException):
    status_code = 409

class HttpException(Exception):
    status_code = 500

    def __init__(self, message="", status_code=None):
        super().__init__(message)
"""


def tree(root: Path, files: dict[str, str]) -> None:
    """Write a package tree: every directory on a file's path is a package."""
    for relative, source in {"app/exceptions/__init__.py": EXCEPTIONS, **files}.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        for package in path.relative_to(root).parents:
            if package != Path("."):
                (root / package / "__init__.py").touch()
        path.write_text(source, encoding="utf-8")


def errors(root: Path, action: str, **options) -> set[ControllerResponse]:
    """The error variants ``ThingController@<action>`` documents."""
    contracts = ControllerContractExtractor(
        root / "app" / "controllers", STATUSES, **options
    ).extract()
    return {
        variant
        for variant in contracts[f"ThingController@{action}"].responses
        if variant.kind == "error"
    }


def error(status: int) -> ControllerResponse:
    return ControllerResponse(status, "error")


# The route-fact tests' framework: a ``Request`` whose method the route fixes,
# the ``run_in_thread`` forwarder, and an ``authorize`` whose billing half
# runs only for an unsafe method and a billed key (``Access`` in the API).
ROUTE_STATUSES = {**STATUSES, "Forbidden": 403, "PaymentRequired": 402}

ROUTE_FRAMEWORK = {
    "cara/http/request/Request.py": """
class Request:
    @property
    def method(self) -> str:
        return "GET"
""",
    "cara/context/ExecutionContext.py": """
class ExecutionContext:
    @staticmethod
    def bind_to_thread(func, *args, **kwargs):
        return func

    @staticmethod
    async def run_in_thread(func, *args, **kwargs):
        return await loop.run_in_executor(
            None, ExecutionContext.bind_to_thread(func, *args, **kwargs)
        )
""",
    "app/support/Errors.py": """
from app.exceptions import ServiceException


class Forbidden(ServiceException):
    status_code = 403


class PaymentRequired(ServiceException):
    status_code = 402
""",
    "app/support/Access.py": """
from app.support.Errors import Forbidden, PaymentRequired


def has_permission(request, key):
    return lookup(request, key)


def billing_writable_request(request):
    raise PaymentRequired("Held.")


def authorize(request, key, *, billing=True):
    if not has_permission(request, key):
        raise Forbidden(f"Missing permission: {key}")
    if billing:
        _assert_mutation_writable(request, key)


def _assert_mutation_writable(request, key):
    if str(getattr(request, "method", "GET") or "GET").upper() not in {
        "GET",
        "HEAD",
        "OPTIONS",
    } and key in {"things.manage"}:
        billing_writable_request(request)
""",
}


def route_errors(root: Path, routes: list[dict], action: str) -> set[int]:
    """The error statuses ``ThingController@<action>`` documents under
    ``routes``."""
    contracts = ControllerContractExtractor(
        root / "app" / "controllers", ROUTE_STATUSES, routes=routes
    ).extract()
    return {
        variant.status
        for variant in contracts[f"ThingController@{action}"].responses
        if variant.kind == "error"
    }


def routes_for(**methods: str) -> list[dict]:
    """``ThingController`` route rows: ``action=METHOD``."""
    return [
        {"method": method, "controller": "ThingController", "action": action}
        for action, method in methods.items()
    ]
