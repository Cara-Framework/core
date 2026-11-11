"""The typed errors an action answers with are part of its published contract.

Live 2026-11-11 (Synkronus doors sweep): a publish already running answered
409, a refused shipment 400, a missing photo 404 and an advertising read on a
marketplace without ads 400 — and none of them was in the generated document,
which listed only the middleware statuses and a blanket 422. An action now
documents every typed exception its own methods raise, and the ones a service
raises below it through an ``@raises(...)`` docstring declaration.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cara.openapi import (
    ControllerContract,
    ControllerContractExtractor,
    ControllerResponse,
    ErrorContractExtractor,
    SpecInfo,
    UnknownDeclaredError,
    build_spec,
)

_STATUSES = {"InvalidInput": 400, "EntityNotFound": 404, "ConflictException": 409}


def _contract(tmp_path: Path, source: str, action: str = "store") -> set:
    directory = tmp_path / "controllers"
    directory.mkdir()
    (directory / "ThingController.py").write_text(source, encoding="utf-8")
    contracts = ControllerContractExtractor(directory, _STATUSES).extract()
    return set(contracts[f"ThingController@{action}"].responses)


def test_an_error_the_action_raises_through_its_own_methods_is_documented(tmp_path):
    source = """
class ThingController:
    async def store(self, request, response):
        await self._open(request)
        return response.envelope(row, status=202)

    async def _open(self, request):
        if running:
            raise ConflictException("A publish is already running.")
        try:
            claim()
        except ValueError as exc:
            raise exc
"""
    assert _contract(tmp_path, source) == {
        ControllerResponse(202, "envelope"),
        ControllerResponse(409, "error"),
    }


def test_a_declared_error_from_below_the_controller_is_documented(tmp_path):
    source = '''
class ThingController:
    async def store(self, request, response):
        """Ship it.

        @raises(InvalidInput, ConflictException)
        """
        return response.envelope(await self.service.ship(request))
'''
    assert _contract(tmp_path, source) == {
        ControllerResponse(200, "envelope"),
        ControllerResponse(400, "error"),
        ControllerResponse(409, "error"),
    }


def test_a_declaration_on_a_shared_helper_reaches_every_action_that_calls_it(tmp_path):
    source = '''
class ThingController:
    async def show(self, request, response):
        thing = await self._thing(request)
        return response.envelope(thing)

    async def _thing(self, request):
        """@raises(InvalidInput)"""
        return await self.service.find(request)
'''
    assert ControllerResponse(400, "error") in _contract(tmp_path, source, "show")


def test_a_status_the_action_answers_itself_keeps_its_own_words(tmp_path):
    source = """
class ThingController:
    async def store(self, request, response):
        if missing:
            raise EntityNotFound("Gone.")
        return response.json({"error": "gone", "type": "not_found"}, 404)
"""
    statuses = _contract(tmp_path, source)
    assert ControllerResponse(404, "json") in statuses
    assert ControllerResponse(404, "error") not in statuses


def test_an_unknown_declaration_fails_the_build(tmp_path):
    source = '''
class ThingController:
    async def store(self, request, response):
        """@raises(InvalidInptu)"""
        return response.envelope(row)
'''
    with pytest.raises(UnknownDeclaredError, match="InvalidInptu"):
        _contract(tmp_path, source)


def test_an_exception_s_status_is_its_own_or_its_nearest_base_s(tmp_path):
    framework = tmp_path / "framework"
    framework.mkdir()
    (framework / "Base.py").write_text(
        "class HttpError(Exception):\n    status_code = 500\n"
        "class ValidationException(HttpError):\n    status_code = 422\n",
        encoding="utf-8",
    )
    app = tmp_path / "app"
    app.mkdir()
    (app / "Errors.py").write_text(
        "class InvalidInput(HttpError):\n    status_code = 400\n"
        "class PriceRefused(InvalidInput):\n    pass\n"
        "class ValidationException(HttpError):\n    status_code = 400\n"
        "class Plain(Exception):\n    pass\n",
        encoding="utf-8",
    )

    statuses = ErrorContractExtractor(
        (app,), framework_roots=(framework,)
    ).exception_statuses()

    assert statuses["PriceRefused"] == 400
    assert statuses["HttpError"] == 500
    # The application's class of a framework name is the one it raises.
    assert statuses["ValidationException"] == 400
    assert "Plain" not in statuses


def test_a_raised_error_is_published_as_an_error_body():
    spec = build_spec(
        info=SpecInfo(title="T", description="D"),
        schemas={},
        mapping={},
        routes=[
            {
                "method": "POST",
                "path": "/api/things/",
                "controller": "ThingController",
                "action": "store",
                "name": "things.store",
            }
        ],
        envelope_components={"_Meta": {"type": "object"}, "ApiErrorBody": {}},
        controller_contracts={
            "ThingController@store": ControllerContract(
                requests=(),
                responses=(
                    ControllerResponse(202, "envelope"),
                    ControllerResponse(409, "error"),
                ),
            )
        },
    )

    conflict = spec["paths"]["/api/things/"]["post"]["responses"]["409"]
    assert conflict["description"] == "Conflict."
    assert conflict["x-http-status"] == 409
    assert conflict["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/ApiErrorBody"
    }
